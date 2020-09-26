from collections import namedtuple
from parsers.pa_pdf_parser import PDFStringIterator


LAST_ROW_PRECINCT = 'Totals'

HEADER_TO_OFFICE = {
    'PRESIDENT OF THE UNITED STATES': 'President',
    'REPRESENTATIVE IN CONGRESS': 'U.S. House',
    'SENATOR IN THE GENERAL ASSEMBLY': 'State Senate',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY': 'General Assembly',
}

PARTIES = {
    'DEM',
    'REP',
}

Candidate = namedtuple('Candidate', 'office district party name')
ParsedRow = namedtuple('ParsedRow', 'county precinct office district party candidate votes')


class ElectionwareOffice:
    _valid_headers = None
    _offices_with_districts = None

    def __init__(self, s):
        self.name = s
        self.district = ''
        self.party = ''
        self.extract_party()

    def append(self, s):
        if not self.name:
            self.name = s
        else:
            self.name += ' ' + s
        self._trim_spaces()
        return self

    def extract_party(self):
        if self.name in PARTIES:
            self.party = self.name
            self.name = ''
        elif ' ' in self.name:
            prefix, suffix = self.name.split(' ', 1)
            if prefix in PARTIES:
                self.party, self.name = prefix, suffix

    def extract_district(self):
        for office_with_district in self._offices_with_districts:
            if office_with_district in self.name:
                self.district = self._offices_with_districts.get(office_with_district)
                if self.district is None:
                    self.name, self.district = self.name.rsplit(' ', 1)
                    if self.district == 'DISTRICT':
                        self.name, self.district = self.name.rsplit(' ', 1)
                    for stripped_string in ('ST', 'ND', 'RD', 'TH', 'L', 'C'):
                        self.district = self.district.replace(stripped_string, '')
                    self.district = int(self.district)

    def is_valid(self):
        return self.name in self._valid_headers

    def is_terminal(self):
        return self.name.startswith('VOTE FOR') or self.name == 'STATISTICS'

    def normalize(self):
        self.name = HEADER_TO_OFFICE.get(self.name, self.name)

    def _trim_spaces(self):
        self.name = self.name.replace('STATISTI CS', 'STATISTICS')\
            .replace('REPRESE NTATIVE', 'REPRESENTATIVE')\
            .replace('ASSEMBL Y', 'ASSEMBLY')\
            .replace('DELEGAT E', 'DELEGATE')\
            .replace('  ', ' ')


class ElectionwarePDFTableHeaderParser(PDFStringIterator):
    _office_clazz = None

    def __init__(self, strings, previous_table_header):
        super().__init__(strings)
        self._candidates = None
        self._previous_table_header = previous_table_header

    def get_candidates(self):
        if self._candidates is None:
            self._parse()
        return self._candidates

    def get_table_headers(self):
        if self._candidates is None:
            self._parse()
        return self.get_processed_strings()

    def _parse(self):
        offices = list(self._parse_headers())
        self._candidates = list(self._parse_subheaders(offices))

    def _parse_headers(self):
        raise NotImplementedError

    def _parse_subheaders(self, offices):
        for office in offices:
            yield from self._parse_subheader(office)

    def _parse_subheader(self, office):
        raise NotImplementedError

    def _process_next_header_string(self, office):
        s = self._get_next_string()
        if not office:
            return self._office_clazz(s)
        return office.append(s)

    def _process_next_subheader_string(self, candidate):
        s = self._get_next_string()
        if self._ignorable_string(s):
            return None
        if not candidate:
            return s
        return candidate + ' ' + s

    @staticmethod
    def _ignorable_string(s):
        return s == '-' or s.startswith('VOTE FOR')

    @classmethod
    def _process_candidate(self, candidate, office):
        raise NotImplementedError


class ElectionwarePDFTableBodyParser(PDFStringIterator):
    _first_footer_substring = None
    _county = None

    def __init__(self, strings, candidates):
        super().__init__(strings)
        self._candidates = candidates
        self._table_is_done = False

    def __iter__(self):
        while not self._table_is_done and not self._page_is_done():
            precinct = self._get_next_string()
            yield from self._parse_row(precinct)

    def table_is_done(self):
        return self._table_is_done

    def _page_is_done(self):
        return self._peek_next_string().startswith(self._first_footer_substring)

    def _parse_row(self, precinct):
        self._table_is_done = precinct == LAST_ROW_PRECINCT
        for candidate in self._candidates:
            if self._table_is_done and self._page_is_done():
                # totals row may not necessarily contain every column
                break
            votes = self._get_next_string()
            if not self._table_is_done and not self._candidate_is_invalid(candidate):
                votes = int(votes.replace(',', ''))
                yield ParsedRow(self._county, precinct.title(), candidate.office.title(),
                                candidate.district, candidate.party,
                                candidate.name.title(), votes)

    @classmethod
    def _candidate_is_invalid(self, candidate):
        return False


class ElectionwarePDFPageParser:
    _standard_header = None
    _first_footer_substring = None
    _table_header_parser_clazz = None
    _table_body_parser_clazz = None

    def __init__(self, page, previous_table_header):
        strings = page.get_strings()
        self._table_body_parser = None
        self._previous_table_header = previous_table_header
        self._init_header(strings)

    def __iter__(self):
        while not self.page_is_done():
            candidates = self._process_table_headers()
            if not candidates:
                # any page without valid candidates has no additional
                # tables and is therefore skippable
                break
            yield from self.process_table_body(candidates)

    def page_is_done(self):
        return self._strings[0].startswith(self._first_footer_substring)

    def get_continued_table_header(self):
        if not self._table_body_parser or self._table_body_parser.table_is_done():
            return None
        return self._table_headers

    def _init_header(self, strings):
        header = strings[:len(self._standard_header)]
        assert (header == self._standard_header)
        self._strings = strings[len(self._standard_header):]

    def _process_table_headers(self):
        table_header_parser = self._table_header_parser_clazz(self._strings, self._previous_table_header)
        self._table_headers = table_header_parser.get_table_headers()
        self._strings = table_header_parser.get_remaining_strings()
        return table_header_parser.get_candidates()

    def process_table_body(self, candidates):
        self._table_body_parser = self._table_body_parser_clazz(self._strings, candidates)
        yield from iter(self._table_body_parser)
        self._strings = self._table_body_parser.get_remaining_strings()


def pdf_to_csv(pdf, csv_writer, pdf_page_parser_clazz):
    csv_writer.writeheader()
    previous_table_header = None
    for page in pdf:
        print(f'processing page {page.get_page_number()}')
        pdf_page_parser = pdf_page_parser_clazz(page, previous_table_header)
        for row in pdf_page_parser:
            csv_writer.writerow(row._asdict())
        previous_table_header = pdf_page_parser.get_continued_table_header()
