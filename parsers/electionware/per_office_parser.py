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


class ElectionwarePerOfficePDFStringIterator(PDFStringIterator):
    _first_footer_substring = None

    def page_is_done(self):
        s = self.peek()
        return s.startswith(self._first_footer_substring)


class ElectionwarePerOfficePDFTableHeaderParser:
    _office_clazz = None

    def __init__(self, string_iterator, previous_table_header):
        self._string_iterator = string_iterator
        self._candidates = None
        self._previous_table_header = previous_table_header
        self._processed_strings = []

    def get_candidates(self):
        if self._candidates is None:
            self._parse()
        return self._candidates

    def get_table_headers(self):
        if self._candidates is None:
            self._parse()
        return self._processed_strings

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
        s = self._next_string()
        if not office:
            return self._office_clazz(s)
        return office.append(s)

    def _process_next_subheader_string(self, candidate):
        s = self._next_string()
        if self._ignorable_string(s):
            return None
        if not candidate:
            return s
        return candidate + ' ' + s

    def _next_string(self):
        s = next(self._string_iterator)
        self._processed_strings.append(s)
        return s

    @staticmethod
    def _ignorable_string(s):
        return s == '-' or s.startswith('VOTE FOR')

    @classmethod
    def _process_candidate(cls, candidate, office):
        raise NotImplementedError


class ElectionwarePerOfficePDFTableBodyParser:
    _county = None

    def __init__(self, string_iterator, candidates):
        self._string_iterator = string_iterator
        self._candidates = candidates
        self._table_is_done = False

    def __iter__(self):
        while not self._table_is_done and not self._string_iterator.page_is_done():
            precinct = next(self._string_iterator)
            yield from self._parse_row(precinct)

    def table_is_done(self):
        return self._table_is_done

    def _parse_row(self, precinct):
        self._table_is_done = precinct == LAST_ROW_PRECINCT
        for candidate in self._candidates:
            if self._table_is_done and self._string_iterator.page_is_done():
                # totals row may not necessarily contain every column
                break
            votes = next(self._string_iterator)
            if not self._table_is_done and not self._candidate_is_invalid(candidate):
                votes = int(votes.replace(',', ''))
                yield ParsedRow(self._county, precinct.title(), candidate.office.title(),
                                candidate.district, candidate.party,
                                candidate.name.title(), votes)

    @classmethod
    def _candidate_is_invalid(cls, candidate):
        return False


class ElectionwarePerOfficePDFPageParser:
    _pdf_string_iterator_clazz = None
    _table_header_parser_clazz = None
    _table_body_parser_clazz = None
    _header = None

    def __init__(self, page, previous_table_header):
        self._table_body_parser = None
        self._previous_table_header = previous_table_header
        self._string_iterator = self._pdf_string_iterator_clazz(self._get_strings(page))
        self._verify_header()

    def __iter__(self):
        while not self._string_iterator.page_is_done():
            candidates = self._process_table_headers()
            if not candidates:
                # any page without valid candidates has no additional
                # tables and is therefore skippable
                break
            yield from self.process_table_body(candidates)

    def get_continued_table_header(self):
        if not self._table_body_parser or self._table_body_parser.table_is_done():
            return None
        return self._table_headers

    def _get_strings(self, page):
        return page.get_strings()

    def _verify_header(self):
        header = [next(self._string_iterator) for _ in range(len(self._header))]
        assert header == self._header

    def _process_table_headers(self):
        table_header_parser = self._table_header_parser_clazz(self._string_iterator, self._previous_table_header)
        self._table_headers = table_header_parser.get_table_headers()
        return table_header_parser.get_candidates()

    def process_table_body(self, candidates):
        self._table_body_parser = self._table_body_parser_clazz(self._string_iterator, candidates)
        yield from iter(self._table_body_parser)


def electionware_per_office_pdf_to_csv(pdf, csv_writer, pdf_page_parser_clazz):
    csv_writer.writeheader()
    previous_table_header = None
    for page in pdf:
        print(f'processing page {page.get_page_number()}')
        pdf_page_parser = pdf_page_parser_clazz(page, previous_table_header)
        for row in pdf_page_parser:
            csv_writer.writerow(row._asdict())
        previous_table_header = pdf_page_parser.get_continued_table_header()
