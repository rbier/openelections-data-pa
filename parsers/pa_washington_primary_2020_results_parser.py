import os
from parsers.electionware.csv import write_electionware_pdf_to_csv
from parsers.electionware.data_source import FileSource
from parsers.electionware.row_filters import SpecificWriteInCandidatesFilter
from parsers.electionware.row_transformers import CandidateTitleCaseTransformer, OfficeTitleCaseTransformer

SOURCE_FILE = os.path.join('..', '..', 'openelections-sources-pa', '2020',
                           'Washington PA 2020 Primary Precinct Summary.pdf')

CONFIGURATION = {
    'data_source': [
        FileSource(SOURCE_FILE)
    ],
    'election_description': {
        'county': 'Washington',
        'state_abbrev': 'PA',
        'yyyymmdd': '20200602',
        'type': 'primary',
    },
    'page_structure': {
        'expected_header': [
            '',
            'Summary Results Report',
            '2020 Presidential Primary',
            'June 2, 2020',
            'OFFICIAL RESULTS',
            'Washington',
        ],
        'expected_footer': 'Precinct Summary - 06/23/2020',
        'table_headers': [
            ['TOTAL', 'Election Day', 'Absentee', 'Provisional', 'Military',],
            ['TOTAL', 'Election Day', 'Military', 'Provisional', 'Absentee',],
        ],
        'has_vote_percent_column': True
    },
    'table_processing': {
        'extra_row_transformers': [
            CandidateTitleCaseTransformer(),
            OfficeTitleCaseTransformer()
        ],
        'extra_row_filters': [
            SpecificWriteInCandidatesFilter()
        ],
        'raw_office_to_office_and_district': {
            'PRESIDENT OF THE UNITED STATES': ('President', ''),
            'REPRESENTATIVE IN CONGRESS': ('U.S. House', 14),
            'REPRESENTATIVE IN THE GENERAL ASSEMBLY 15TH DISTRICT': ('General Assembly', 15),
            'REPRESENTATIVE IN THE GENERAL ASSEMBLY 39TH DISTRICT': ('General Assembly', 39),
            'REPRESENTATIVE IN THE GENERAL ASSEMBLY 40TH DISTRICT': ('General Assembly', 40),
            'REPRESENTATIVE IN THE GENERAL ASSEMBLY 46TH DISTRICT': ('General Assembly', 46),
            'REPRESENTATIVE IN THE GENERAL ASSEMBLY 48TH DISTRICT': ('General Assembly', 48),
            'REPRESENTATIVE IN THE GENERAL ASSEMBLY 49TH DISTRICT': ('General Assembly', 49),
            'REPRESENTATIVE IN THE GENERAL ASSEMBLY 50TH DISTRICT': ('General Assembly', 50),
            'SENATOR IN THE GENERAL ASSEMBLY 37TH DISTRICT': ('State Senate', 37),
        },
        'openelections_mapped_header': [
            'votes', 'election_day', 'absentee', 'provisional', 'military',
        ],
    },
}


if __name__ == "__main__":
    write_electionware_pdf_to_csv(CONFIGURATION)
