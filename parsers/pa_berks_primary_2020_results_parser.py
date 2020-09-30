import csv
import os
from parsers.constants.pa_candidates_2020 import STATEWIDE_PRIMARY_CANDIDATES
from parsers.electionware_pdf_parser_variant import pdf_to_csv, Candidate, ElectionwareOffice, \
    ElectionwarePDFPageParser, ElectionwarePDFTableBodyParser, ElectionwarePDFTableHeaderParser
from parsers.pa_pdf_parser import PDFPageIterator


COUNTY = 'Berks'

OUTPUT_FILE = os.path.join('..', '2020', '20200602__pa__primary__berks__precinct.csv')
OUTPUT_HEADER = ['county', 'precinct', 'office', 'district', 'party', 'candidate', 'votes']

BERKS_FILE = os.path.join('..', '..', 'openelections-sources-pa', '2020',
                          'Berks PA 2020 Primary Precinct Results.pdf')
BERKS_HEADER = [
    '',
    '2020 PRIMARY',
    'June 2, 2020',
    'BERKS COUNTY',
    'CERTIFIED RESULTS',
    'Precinct Report',
]

TERMINAL_SUBHEADER_STRINGS = ('Ballots Cast - Total', 'Write-in Totals')
FIRST_FOOTER_SUBSTRING = 'Custom Table Report - 06/22/2020'

VALID_HEADERS = {
    'STATISTICS',
    'PRESIDENT OF THE UNITED STATES',
    'ATTORNEY GENERAL',
    'AUDITOR GENERAL',
    'STATE TREASURER',
    'REPRESENTATIVE IN CONGRESS C04',
    'REPRESENTATIVE IN CONGRESS 6TH DISTRICT',
    'REPRESENTATIVE IN CONGRESS 9TH DISTRICT',
    'SENATOR IN THE GENERAL ASSEMBLY 11TH DISTRICT',
    'SENATOR IN THE GENERAL ASSEMBLY 29TH DISTRICT',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY L5',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY L124',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY L126',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY L127',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY L128',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY L129',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY L130',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY L134',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY 187TH DISTRICT',
    'DELEGATE TO THE DEMOCRATIC NATIONAL CONVENTION C04',
    'DELEGATE TO THE DEMOCRATIC NATIONAL CONVENTION 6TH DISTRICT',
    'DELEGATE TO THE DEMOCRATIC NATIONAL CONVENTION 9TH DISTRICT',
    'ALTERNATE DELEGATE TO THE DEMOCRATIC NATIONAL CONVENTION C04',
    'ALTERNA TE DELEGATE TO THE DEMOCRA TIC NATIONA L CONVENT ION 6TH DISTRICT',
    'DELEGATE TO THE REPUBLICAN NATIONAL CONVENTION C04',
    'DELEGATE TO THE REPUBLICAN NATIONAL CONVENTION 6TH DISTRICT',
    'DELEGATE TO THE REPUBLICAN NATIONAL CONVENTION 9TH  DISTRICT',
    'ALTERNATE DELEGATE TO THE REPUBLICAN NATIONAL CONVENTION 4',
    'ALTERNATE DELEGATE TO THE REPUBLICAN NATIONAL CONVENTION 6',
    'ALTERNATE DELEGATE TO THE REPUBLICAN NATIONAL CONVENTION 9',
}

OFFICES_WITH_DISTRICTS = {
    'REPRESENTATIVE IN CONGRESS': None,
    'SENATOR IN THE GENERAL ASSEMBLY': None,
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY': None,
}

VALID_SUBHEADERS = STATEWIDE_PRIMARY_CANDIDATES | {
    'Ballots Cast - Total',
    'Write-in Totals',
    'MADELEINE DEAN',
    'CHRISSY HOULAHAN',
    'LAURA QUICK',
    'GARY WEGMAN',
    'JUDY SCHWANK',
    'GRAHAM GONZALES',
    'TAYLOR PICONE',
    'MARK ROZZI',
    'ROBIN COSTENBADER- JACOBSON',
    'MANNY GUZMAN',
    'ROBERT MELENDEZ',
    'RAYMOND EDWARD BAKER',
    'CESAR CEPEDA',
    'KELLY MCDONOUGH',
    'LAMAR FOLEY',
    'MICHAEL BLICHAR, JR.',
    'KATHY BARNETTE',
    'JOHN EMMONS',
    'VINCENT D. GAGLIARDO, JR.',
    'BRANDEN MOYER',
    'MARK M. GILLEN',
    'JIM COX',
    'DAVID M. MALONEY',
    'RYAN E. MACKENZIE',
    'GARY DAY',
    'LEANNE BURCHIK',
    'LIZ BETTINGER',
    'VINCE DEMELFI',
    'JIM SAFFORD',
    'PHILA BACK',
    'CATHERINE MAHON',
    'SANDRA WATERS',
    'DAN MEUSER',
    'ANNETTE C. BAKER',
    'DAVE ARGALL',
    'BARRY JOZWIAK',
    'JERRY KNOWLES',
    'JAMES D. OSWALD',
}


class BerksOffice(ElectionwareOffice):
    _valid_headers = VALID_HEADERS
    _offices_with_districts = OFFICES_WITH_DISTRICTS

    def should_be_recorded(self):
        return 'DELEGATE' not in self.name


class BerksPDFTableHeaderParser(ElectionwarePDFTableHeaderParser):
    _office_clazz = BerksOffice

    def _parse_headers(self):
        office = None
        while not (office and office.is_terminal()):
            office = self._process_next_header_string(office)
            if office.is_valid():
                office.extract_district()
                if office.should_be_recorded():
                    office.normalize()
                    yield office
                if not office.is_terminal():
                    office = None

    def _parse_subheaders(self, offices):
        for office in offices:
            candidate = None
            while not (candidate and candidate in TERMINAL_SUBHEADER_STRINGS):
                candidate = self._process_next_subheader_string(candidate)
                if candidate in VALID_SUBHEADERS:
                    yield from self._process_candidate(candidate, office)
                    if candidate not in TERMINAL_SUBHEADER_STRINGS:
                        candidate = None

    @classmethod
    def _process_candidate(self, candidate, office):
        if office.name == 'STATISTICS':
            assert(candidate == 'Ballots Cast - Total')
            yield Candidate('Ballots Cast', '', '', '')
        else:
            if candidate == 'Write-in Totals':
                candidate = 'Write-in'
            if candidate == 'ROBIN COSTENBADER- JACOBSON':
                candidate = 'ROBIN COSTENBADER-JACOBSON'
            yield Candidate(office.name, office.district, office.party, candidate)


class BerksPDFTableBodyParser(ElectionwarePDFTableBodyParser):
    _first_footer_substring = FIRST_FOOTER_SUBSTRING
    _county = COUNTY


class BerksPDFPageParser(ElectionwarePDFPageParser):
    _standard_header = BERKS_HEADER
    _first_footer_substring = FIRST_FOOTER_SUBSTRING
    _table_header_parser_clazz = BerksPDFTableHeaderParser
    _table_body_parser_clazz = BerksPDFTableBodyParser

    def _init_header(self, strings):
        if 'CITY OF READING QUESTIONS' in strings[3:5]:
            # skip these pages; different format than others and are amendment questions
            self._strings = [FIRST_FOOTER_SUBSTRING]
        else:
            super()._init_header(strings)


if __name__ == "__main__":
    with open(OUTPUT_FILE, 'w', newline='') as f:
        pdf_to_csv(PDFPageIterator(BERKS_FILE),
                   csv.DictWriter(f, OUTPUT_HEADER),
                   BerksPDFPageParser)
