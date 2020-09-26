from collections import namedtuple
import csv
import os
from parsers.pa_pdf_parser import PDFPageIterator, PDFStringIterator
from parsers.constants.pa_candidates_2020 import STATEWIDE_PRIMARY_CANDIDATES


# Uses Electionware Custom Table Report PDF format
COUNTY = 'Indiana'

OUTPUT_FILE = os.path.join('..', '2020', '20200602__pa__primary__indiana__precinct.csv')
OUTPUT_HEADER = ['county', 'precinct', 'office', 'district', 'party', 'candidate', 'votes']

INDIANA_FILE = os.path.join('..', '..', 'openelections-sources-pa', '2020',
                          'Indiana PA June 2 Elections Results.pdf')
INDIANA_HEADER = [
    '',
    'PPAIND20',
    'June, 2, 2020',
    'Indiana County',
    'OFFICIAL RESULTS',
    'Custom Table Report',
]

FIRST_FOOTER_SUBSTRING = 'June 2, 2020 General Primary Election'
FIRST_ROW_PRECINCT = 'ARMAGH'
LAST_ROW_PRECINCT = 'Totals'

VALID_HEADERS = {
    'STATISTICS',
    'PRESIDENT OF THE UNITED STATES',
    'ATTORNEY GENERAL',
    'AUDITOR GENERAL',
    'STATE TREASURER',
    'REPRESENTATIVE IN CONGRESS',
    'SENATOR IN THE GENERAL ASSEMBLY',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY 55TH DISTRICT',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY 60TH DISTRICT',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY 62ND DISTRICT',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY 66TH DISTRICT',
    'DELEGATE TO THE DEMOCRATIC NATIONAL CONVENTION',
    'DELEGATE TO THE REPUBLICAN NATIONAL CONVENTION',
    'ALTERNATE DELEGATE TO THE REPUBLICAN NATIONAL CONVENTION',
}

HEADER_TO_OFFICE  = {
    'PRESIDENT OF THE UNITED STATES': 'President',
    'REPRESENTATIVE IN CONGRESS': 'U.S. House',
    'SENATOR IN THE GENERAL ASSEMBLY': 'State Senate',
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY': 'General Assembly',
}

OFFICES_WITH_DISTRICTS = {
    'REPRESENTATIVE IN CONGRESS': 15,
    'SENATOR IN THE GENERAL ASSEMBLY': 41,
    'REPRESENTATIVE IN THE GENERAL ASSEMBLY': None,
}

PARTIES = {
    'DEM',
    'REP',
}

PARTY_TO_ABBREVIATION = {
    'Total': '',
    'DEMOCRATIC': 'DEM',
    'REPUBLICAN': 'REP',
    'NONPARTISAN': 'NPA',
}

SKIPPED_HEADER_STRINGS = [
    'VOTE FOR',
    ' of ',
    'Precincts',
    'Reporting',
]

VALID_SUBHEADERS = STATEWIDE_PRIMARY_CANDIDATES | {
    'Registered Voters - Total',
    'Registered Voters - DEMOCRATIC',
    'Registered Voters - REPUBLICAN',
    'Registered Voters - NONPARTISAN',
    'Ballots Cast - Total',
    'Ballots Cast - DEMOCRATIC',
    'Ballots Cast - REPUBLICAN',
    'Ballots Cast - NONPARTISAN',
    'Ballots Cast - Blank',
    'Voter Turnout - Total',
    'Voter Turnout - DEMOCRATIC',
    'Voter Turnout - REPUBLICAN',
    'Voter Turnout - NONPARTISAN',
    'Bernie Sanders',
    'Joseph R. Biden',
    'Tulsi Gabbard',
    'Write-in: M. Bloomburg',
    'Write-in: Amy Klobuchar',
    'Write-in: Don Blankenship',
    'Write-in: Andrew Yang',
    'Write-in: Mark Cuban',
    'Write-in: Elizabeth Warren',
    'Write-in: Colin Powell',
    'Write-in: Pete Buttigieg',
    'Write-in: Donald J. Trump',
    'Write-in: Donald Trump',
    'Write-in: Michael Bloomberg',
    'Write-in: Corey Booker',
    'Write-in: Donald Trumph',
    'Write-in: Trump Donald',
    'Write-in: D. Trump',
    'Write-in: Trump',
    'Write-in: Mike Bloomberg',
    'Write-in: Sen Kamala Harris',
    'Write-in: Clayton Lamer',
    'Write-in: Andrew Cuomo',
    'Write-in: Nancy Pelosi',
    'Write-in: Gov Cuomo',
    'Write-in: Lucas A. Cruikshank',
    'Write-in: Joe Pisano',
    'Write-in: President Trump',
    'Write-in: Collin Peterson',
    'Write-in: Don Trump',
    'Write-in: D. J. Trump',
    'Josh Shapiro',
    'Write-in: Tom Mattee, Sr.',
    'Write-in: John Lias',
    'Write-in: John Peck',
    'Write-in: Don White',
    'Write-in: Patrick Dougherty',
    'Write-in: Dick Cummings',
    'Write-in: Michael Galasi',
    'Write-in: Barr',
    'Write-in: Timothy Defoor',
    'Write-in: Timothy DeFoor',
    'Write-in: Lucas Cressler',
    'Write-in: Brad McClure',
    'Write-in: Dave Smith',
    'Write-in: Tim Defoor',
    'Write-in: Tim DeFoor',
    'Write-in: Brian Sims',
    'Write-in: Hoseph Pittman',
    'Write-in: John Rafferty Jr.',
    'Write-in: Heather Heidelbaugh',
    'H. Scott Conklin',
    'Michael Lamb',
    'Tracie Fountain',
    'Rose Rosie Marie Davis',
    'Nina Ahmad',
    'Christina M. Hartman',
    'Write-in: Walter Jenkins',
    'Write-in: Josh Shapiro',
    'Write-in: Victoria Tantlinger',
    'Write-in: Charles Crossland',
    'Write-in: Robert Williams',
    'Write-in: Chris Dush',
    'Joe Torsella',
    'Write-in: Glenn GT Thompson',
    'Write-in: Kathy Koontz',
    'Write-in: Sam Black',
    'Write-in: Becky Wallace',
    'Write-in: Not Assigned',
    'Write-in: Scott Burba',
    'Robert Williams',
    'Write-in: Lori Pavic',
    'Write-in: James Struzzi',
    'Write-in: James Struzzi II',
    'Write-in: Struzzi, Jim',
    'Write-in: Sam Shutter',
    'Write-in: Calvin Masilela',
    'Write-in: Gerald Smith',
    'Write-in: John Jake Matson',
    'Write-in: Susan Kelly',
    'Write-in: Bryan Smith',
    'Christina Joy Fulton',
    'Write-in: Sandra Chero',
    'Write-in: Josie Cunningham',
    'Write-in: Jim Vasilko',
    'Write-in: Michael LaMantia',
    'Write-in: Carol Anderson',
    'Donald J. Trump',
    'Write-in: Mitch Daniels',
    'Write-in: Michelle OBama',
    'Write-in: Andrew Bryan Seal',
    'Write-in: George Tate',
    'Write-in: Bill Barr',
    'Write-in: Daniel Pikel',
    'Write-in: Alex Brewer',
    'Stacy L. Garrity',
    'Write-in: Theresa Carnahan',
    'Write-in: James Gunter',
    'Write-in: Robert G. Smith',
    'Write-in: Jim Struzzi',
    'Write-in: Joe Biden',
    'Jason Silvis',
    'Write-in: Robert Colgan',
    'Write-in: Michelle Plummer',
    'John Jack Matson',
    'Richard E. Smead',
    'Write-in: Christina Joy Fulton',
    'Write-in: Abigail Carr',
    'Write-in: Robert Kovalcik',
    'Write-in: Chad Frick',
    'Write-in: Randy Bruner',
    'Write-in: Maria Jack',
    'Jennifer Baker',
    'Write-in: Harry Waltermire',
    'Write-in: Bob Brewer',
    'Write-in: Anne Polenik',
    'Write-in: Lslie Rossi',
    'Write-in: Steven Medude',
    'Write-in: Carmen R. Bickings',
    'Write-in: Joseph Lamantia',
    'Write-in: Donna Cupp',
    'Write-in: Michael Baker',
    'Write-in: Noah Schwartz',
    'Write-in: Carrie Ankeny',
    'Write-in: Struzzi',
    'Write-in: Pittman',
    'Write-in: Pittman, Joe',
    'Write-in: Mary Blythe',
    'Write-in: Jeff Widdowson',
    'Write-in: Ellen Bowman',
    'Write-in: Dave Reed',
    'Write-in: James Malone',
    'Write-in: John Jack Matson',
    'Ronald Fairman',
    'Write-in: Simon Bianco',
    'Write-in: Warren Peter',
    'Write-in: Donald Lancaster',
    'Write-in: Darrick S. Johnson',
    'Write-in: Lloyd Smucker',
    'Roque Rocky De La Fuente',
    'Write-in: Biden',
    'Write-in: Larry Hogan',
    'Write-in: John Barbor',
    'Write-in: Rebecca Pizer',
    'Write-in: Fairman',
    'Write-in: Eric Cook',
    'Write-in: Ross Brickelmyer',
    'Write-in: Suzanne Dejeet',
    'Write-in: James E. Gromley',
    'Write-in: Bob Colgan',
    'Write-in: Dennis Semick',
    'Write-in: Stephanie Hawk',
    'Brian Smith',
    'Robert Sheesley',
    'Write-in: Ronald Fairman',
    'Write-in: Amand Nichols',
    'Write-in: Sam Pisarcik',
    'Write-in: Liz Ferguson',
    'Write-in: Evan Marino',
    'Write-in: Beth Ann Seal',
    'Write-in: Joann B. Grosik',
    'Write-in: Cupp',
    'Write-in: Jeff Lieb',
    'Write-in: Ken Layng',
    'Write-in: Michelle Mustello',
    'Write-in: Daniel K. Smith',
    'Write-in: Madison Albright',
    'Write-in: Virginia W. Smith',
    'Write-in: Ronald E. Oswald',
    'Write-in: David Beatty',
    'Write-in: Pat Toomey',
    'Write-in: Joe Struzzi',
    'Write-in: Logan Dellafiora',
    'Write-in: Franic Lui',
    'Write-in: Laura Thomas',
    'Write-in: Conrad Warner',
    'Write-in: Brian A. Smith',
    'Write-in: Phillip Sharrow',
    'Chuck Pascal',
    'Write-in: Terry Noble',
    'Write-in: James Smith',
    'Write-in: Richard E. Smead',
    'Write-in: Joe Black',
    'Write-in: Sean Delaney',
    'Bill Weld',
    'Write-in: Bernie Sanders',
    'Write-in: Michael Pence',
    'Write-in: Amy Smith',
    'Write-in: Joe Torsella',
    'Write-in: Brian Smith',
    'Write-in: Robert E. Single',
    'Write-in: Anthony J. DeLoreto',
    'Write-in: Anthony J. Deloreto',
    'Write-in: John Petrosky',
    'Jim Vasilko',
    'Write-in: Steve Atwood',
    'Write-in: Ron Fairman',
    'Write-in: Jim Wright',
    'Write-in: Ed Chapman',
    'Write-in: John Pittman',
    'Write-in: Doug McCrae',
    'Write-in: Mary A. Riley',
    'Write-in: Jacqueline Cupp',
    'Write-in: Valerie Palmer',
    'Write-in: Chad W. Stiffey',
    'Write-in: Jess Stairs',
    'Write-in: Lawrence Matthews',
    'Write-in: Terry R. Orvash',
    'Write-in: David R Flaherty',
    'Write-in: David J. Cupp',
    'Write-in: J. Cupp',
    'Write-in: Brandi Ports',
    'Write-in: Stacy L. Garrity',
    'Write-in: James P. Smith',
    'Write-in: GT Thompson',
    'Write-in: Pittman Joe',
    'Write-in: Joseph A. Pittman',
    'Write-in: Daniel McGregor',
    'Dennis R. Semsick',
    'Write-in: Janice M. Flaherty',
    'Write-in: Paige Shanner',
    'Write-in: Pamela MacWIlliams',
    'Steve Atwood',
    'Write-in: Nichole Fetterman',
    'Write-in: Gary Blystone',
    'Write-in: Joe Pittman',
    'Write-in: Eric Barker',
    'Write-in: Josh Parsons',
    'Write-in: Rodney Ruddock',
    'Heather Heidelbaugh',
    'Write-in: Eric Shabbick',
    'Write-in: Bob Bell',
    'Write-in: Rose Davis',
    'Write-in: Karen Interrante',
    'Write-in: Frederick J. Boden',
    'Write-in: David Lindahl Jr.',
    'Write-in: Tyler Jon Houck',
    'Write-in: Dennis Semsick',
    'Write-in: Julie Anderson',
    'Ken Layng',
    'Write-in: Rob Nymick',
    'Write-in: Donna Oberland',
    'Write-in: Mark Plants',
    'Write-in: Lynn McCrae',
    'Write-in: Vince Birch',
    'Write-in: Patti Jo Carr',
    'Write-in: Rob Nagle',
    'Write-in: Jackie Cupp',
    'Write-in: Robert Hitchings',
    'Write-in: Kristin Squires',
    'Write-in: Patricia Leach',
    'Write-in: John Shaw',
    'Write-in: Dr. Laura Rhodes',
    'Write-in: Sandy Gillette',
    'Write-in: Matthew Gaudet',
    'Write-in: Stacy Garrity',
    'Write-in: Debbie Lesko',
    'Write-in: Matson',
    'Write-in: Joseph Pittman',
    'Joseph A. Petrarca',
    'Write-in: John Smith',
    'Write-in: Susan Boser',
    'Write-in: Conner Lamb',
    'Write-in: Lynne Alvine',
    'Write-in: Elmer Bland Jr.',
    'Write-in: William Smith',
    'Amanda Nichols',
    'Write-in: Nat Wygonik',
    'Write-in: Jean A. Rickard',
    'Write-in: Bob Casey',
    'Write-in: Allison Blew',
    'Write-in: Vermin Supreme',
    'Write-in: E. Warren',
    'Write-in: Lindsey Graham',
    'Write-in: Wendy Bell',
    'Write-in: Nina Ahmad',
    'Write-in: Heidi Heidlebaugh',
    'Write-in: Donna Oberlander',
    'Write-in: Cris Dush',
    'Write-in: Timothy Card',
    'Jeff Pyle',
    'Write-in: Douglas Steve',
    'Write-in: Bethany Smith',
    'Michele Mustello',
    'Write-in: Brad T. Smith',
    'Write-in: Brandy Henry',
    'Write-in: Pam Plants',
    'Write-in: James Davies',
    'Write-in: Robert S. Bell',
    'Write-in: Jim Kirsh',
    'Write-in: David Noker',
    'Write-in: Joshua Muscatello',
    'Write-in: Michael Sbardella',
    'Write-in: James F. Dawson',
    'Write-in: Brian Moyer',
    'Write-in: Richard Weaverling',
    'Write-in: Mike Hawk',
    'Write-in: Joe Smity',
    'Write-in: Charles Mangus',
    'Write-in: John W. Griffith',
    'Write-in: Eric M. Barker',
    'Write-in: Toby Robert Santik',
    'Write-in: Dush',
    'Abigail Carr',
    'Write-in: Roger George',
    'Write-in: Shanelle Hawk',
    'Write-in: Charles M. Miller',
    'Write-in: Jacquiline Cupp',
    'Write-in: Carl Kologie',
    'Write-in: Mitt Romney',
    'Write-in: Frank Galasso',
    'Write-in: Evan Shultz',
    'Glenn GT Thompson',
    'Write-in: Robert Sheesley',
    'Write-in: Larry Weaver',
    'Write-in: Terry Kerr',
    'Ash Khare',
    'Write-in: David Reed',
    'Write-in: Gilbert Woodley',
    'Write-in: Jeff Pyle',
    'Write-in: Kevin Dickert',
    'Write-in: Chris Enock',
    'Write-in: Douglas Klug',
    'Write-in: Isaac Evans',
    'Write-in: Jim Pittman',
    'Write-in: Dan McGregor',
    'Write-in: Thomas Polenik',
    'Write-in: Bonnie Dunlap',
    'Write-in: Randy Degenkolb',
    'Write-in: Ted Predko',
    'Write-in: Ken Laying',
    'Write-in: Ron Greenawalt',
    'Write-in: Catherine Baker- Knoll',
    'Write-in: Glenn Thompson',
    'Write-in: Christopher Peters',
    'Write-in: Edgar Joy',
    'Write-in: Joe Pitman',
    'Write-in: Barb Peace',
    'Write-in: Michael Galos',
    'Write-in: Michael Galosi',
    'Write-in: John Matson',
    'Write-in: Mike Popson',
    'Write-in: Bernie Smith',
    'Randy Cloak',
    'Write-in: Ann Rae',
    'Write-in: Jennifer Baker',
    'Write-in: Joseph R. Biden Jr.',
    'Write-in: Joseph R. Biden, Jr.',
    'Write-in: Jeff Sessions',
    'Write-in: Paul Ryan',
    'Write-in: Tom Smith',
    'Timothy DeFoor',
    'Write-in: Eugene DePasquale',
    'Write-in: Andrew Plowman',
    'Write-in: Ryan Sharp',
    'Write-in: Anthony DeLoreto',
    'Write-in: Neal Baker',
    'Write-in: Joseh Petrarca',
    'Write-in: Earl Hewitt',
    'Shanelle Hawk',
    'Write-in: JP Habets',
    'Write-in: Doug Mastriano',
    'Write-in: Ned Watkins Sr.',
    'Write-in: Ned Watkins, Sr.',
    'Write-in: Joe Laminitia',
    'Write-in: Alexander Fefalt',
    'Write-in: Rebecca Adams',
    'Write-in: Christine Jack Torretti',
    'Write-in: Bob Clawson',
    'Write-in: Ryan Minniti',
    'Write-in: Scott W. Mossgrove',
    'Write-in: Jr Sabo',
    'Write-in: Arlene Huth',
    'Write-in: David Glass',
    'Write-in: Smith',
    'Write-in: Jay Costa',
    'Write-in: James B. Struzzi',
    'Write-in: Lanord Braughler',
    'Write-in: Ryan Sutter',
    'Write-in: C. Arnold McClure',
    'Write-in: Walt Swalla Jr.',
    'Write-in: Michael Bollinger',
    'Write-in: Joseph R. Biden',
    'Write-in: Marco Rubio',
    'Write-in: John Rafferty',
    'Write-in: Joan Deim',
    'Write-in: H. Scott Conklin',
    'Write-in: Jim Bunt',
    'Write-in: Ted Cunningham',
    'Write-in: Jon Jones',
    'Ann Seal',
    'Write-in: Struzzi Jim',
    'Write-in: Chuck Pascal',
    'Write-in: Jim Day',
    'Write-in: Rod Ruddick',
    'Write-in: Christopher Hanzes',
    'Write-in: Paul Zamba',
    'Write-in: Ash Khare',
    'Write-in: Robin Gorman',
    'Write-in: Hunter Berkavich',
    'Write-in: Angela Greenawalt',
    'Write-in: Patrick McCombie',
    'Write-in: Michael E. Bailey',
    'Write-in: Jim Strussi',
    'Write-in: Glen Thompson',
    'Write-in: Brian Smith',
    'Write-in: Sara Steelman',
    'Write-in: Williams Simmons',
    'Write-in: Tyler S. Nunez',
    'Write-in: William Williams',
    'Write-in: Joseph Kochman',
    'Write-in: Travis Barta',
    'Write-in: Lillian Clemons',
    'Write-in: Denise Jinnings-Doyle',
    'Write-in: Beth Conway',
    'Write-in: Joseph Biden',
    'Write-in: Hunklebee',
    'Write-in: Mike Miller',
    'Write-in: Joseph Torsella',
    'Write-in: Jaqueline Cupp',
    'Write-in: Dennis R. Semsick',
    'Write-in: Tammy Curry',
    'Write-in: Leslie Rossi',
    'Write-in: leslie Rossi',
    'Write-in: Robert A. Thompson',
    'Write-in: Ronald Keith',
    'Write-in: Sam Parisse',
    'Write-in: William Ondo',
    'Write-in: Dirk Berry',
    'Write-in: William M. Malia',
    'Write-in: Abraham E. Kline',
    'Write-in: Rob Sheesley',
    'Write-in: Melissa McCracken',
    'Write-in: Chad Repine',
    'Write-in: Shirley Richardson',
    'Write-in: Matthew Paul Moore',
    'Write-in: John Shimpkus',
    'Write-in: Paul Wass',
    'Anthony J. DeLoreto',
    'Write-in: Jim Watta',
    'Write-in: J. J. Maudie',
    'Write-in: Brian Martin',
    'Write-in: Jack Matson',
    'Write-in: John Lowery',
    'Write-in: Roger Lydick',
    'Write-in: Bill Wise',
    'Write-in: Tony DeLoreto',
    'Write-in: Joe Bidan',
    'Write-in: Josh Shaprio',
    'Write-in: Bill Gastor',
    'Write-in: Tom Kirsh',
    'Write-in: Chris Adams',
    'Write-in: Dan Kelley',
    'Write-in: Justin Spicher',
    'Write-in: Churck Houser',
    'Write-in: Jill Cooper',
    'Write-in: James Allshouse',
    'Write-in: Brett Nesbitt',
    'Write-in: Frederick Houser',
    'Write-in: Ian Brown',
    'Write-in: Danielle Klug',
    'Write-in: James Evans II',
    'Write-in: Charles Albright',
    'Write-in: Zachary Metzler',
    'Write-in: Greg Golden',
    'Write-in: Robert Sheeley',
    'Write-in: Jo Pittman',
    'Write-in: Charles Olson',
    'Write-in: Candice Bolger',
    'Write-in: Barock Obama',
    'Write-in: Sam Smith',
    'Write-in: Samuel Grieggs',
    'Write-in: Dave Senott',
    'Write-in: Joanne Tosti-Vasey',
    'Write-in: Tony Dellafiora',
    'Write-in: Linda Ben-Zvi',
    'Write-in: Rhetta Wilson',
    'Write-in: Mike DeWine',
    'Write-in: Nikki Haley',
    'Write-in: Rand Paul',
    'Write-in: Michael Lamb',
    'Write-in: Christina Hartman',
    'Write-in: Leigh Ann Shabbick',
    'Write-in: Otto Voit',
    'Write-in: Wesley Matthews',
    'Joe Pittman',
    'Write-in: Justin Bines',
    'Jim Struzzi',
    'Write-in: Dan Wilson',
    'Write-in: Linda Gwinn',
    'Write-in: Dylan Murphy',
    'Write-in: Rod Ruddock',
    'Write-in: Matthew Neil',
    'Write-in: Billy Binson',
    'Write-in: Constance Cancelliere',
    'Write-in: John Bonya',
    'Write-in: Glenn T. Thompson',
    'Write-in: Robert E. Smead',
    'Write-in: Thomas Lyttle',
    'Write-in: Kaylee Metzler',
    'Write-in: Jason Stiteler',
    'Write-in: Tay Watenberg',
    'Write-in: Dana Henry',
    'Write-in: James Struzi',
    'Write-in: Alyson Berezansky',
    'Write-in: Michael Lynn',
    'Write-in: Andrew Jones',
    'Write-in: Jim Carr',
    'Write-in: James Smith Jr.',
    'Write-in: Bill Pence',
    'Write-in: John Kasich',
    'Write-in: Mike Pence',
    'Write-in: Glen Weston',
    'Write-in: Gregory Yaworski',
    'Write-in: Dan Rupert',
    'Write-in: Jeff Wood',
    'Write-in: Christine Toretti',
    'Write-in: Pat Leach',
    'Write-in: Ezekiel Welch',
    'Write-in: Matt Gaudet',
    'Write-in: Chris Dusch',
    'Write-in: Rodney Allshouse',
    'Write-in: Eric Shabbicvk',
    'Write-in: Joshua Houser',
    'Write-in: Robert Ofman',
    'Write-in: Dennis Bray',
    'Write-in: Bud Thompson',
    'Write-in: Frank Elling',
    'Write-in: Bernard J. Lieb',
    'Write-in: William Henry',
    'Write-in: Bradley Scott',
    'Write-in: Juanita Hoover',
    'Write-in: William Kerr',
    'Write-in: Tara Binion',
    'Write-in: Pamela Gardner',
    'Write-in: Joanne Bauer',
    'Write-in: Stacy W. Long',
    'Write-in: Sherene Hess',
    'Write-in: Nolan Knarr',
    'Write-in: Paige A. Tomkison',
    'Write-in: Darrell Dunmire',
    'Write-in: Andrew M. Cuomo',
    'Write-in: Ben Carson',
    'Write-in: Thad Montgomery',
    'Write-in: Jennifer Rega',
    'Write-in: Tom Wolfe',
    'Write-in: Shawn Empfield',
    'Write-in: David James Fleming',
    'Write-in: Denny Semsick',
    'Write-in: Christina Fulton',
    'Write-in: Fulton',
    'Write-in: Cathy Degenkolb',
    'Write-in: Brady Rising Jr.',
    'Write-in: Brady Rising, Jr.',
    'Write-in: Timothy W. Roy',
    'Write-in: Matt Fabbri',
    'Write-in: Richard Smead',
    'Write-in: William Darr',
    'Write-in: Ed Legarsky',
    'Write-in: Barbara Barker',
    'Write-in: Donald C. Buterbuagh',
    'Write-in: Andrew Fox',
    'Write-in: Dom Chero',
    'Write-in: Joe Trimarchi',
    'Write-in: Dr. Cybil Pegales Moore',
    'Write-in: DeLoreto',
    'Write-in: Ken Sharp',
    'Write-in: Misty Nocco',
    'Write-in: Dan Crenshaw',
    'Write-in: J. Biden',
    'Write-in: Justin Amash',
    'Write-in: Kim McCullough',
    'Write-in: Robert E.Single',
    'Write-in: R. Diamond',
    'Write-in: Randy Cloak',
    'Write-in: Donald Stitt',
    'Write-in: Carson Greene',
    'Write-in: Diane S. Roy',
    'Write-in: C. Toretti',
    'Write-in: Allen E. Crooks',
    'Write-in: Makolue Polley',
    'Write-in: Richard Gallo',
    'Write-in: Chad Segner',
    'Write-in: Ray McCreary',
    'Write-in: Lyle Dickert',
    'Total Votes Cast',
    'Contest Total',
    'Write-in Totals',
}

GREEDY_SUBHEADER_PAIRS = {
    ('Write-in: Trump', 'Donald'),
    ('Write-in: Pittman', 'Joe'),
    ('Write-in: Struzzi', 'Jim'),
}

Candidate = namedtuple('Candidate', 'office district party name')
ParsedRow = namedtuple('ParsedRow', 'county precinct office district party candidate votes')


class Office:
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

    def extract_party(self):
        if self.name in PARTIES:
            self.party = self.name
            self.name = ''
        elif ' ' in self.name:
            prefix, suffix = self.name.split(' ', 1)
            if prefix in PARTIES:
                self.party, self.name = prefix, suffix

    def extract_district(self):
        for office_with_district in OFFICES_WITH_DISTRICTS:
            if office_with_district in self.name:
                self.district = OFFICES_WITH_DISTRICTS.get(office_with_district)
                if self.district is None:
                    self.name, self.district = self.name.rsplit(' ', 1)
                    if self.district == 'DISTRICT':
                        self.name, self.district = self.name.rsplit(' ', 1)
                    for stripped_string in ('ST', 'ND', 'RD', 'TH'):
                        self.district = self.district.replace(stripped_string, '')
                    self.district = int(self.district)

    def is_valid(self):
        return self.name in VALID_HEADERS

    def normalize(self):
        self.name = HEADER_TO_OFFICE.get(self.name, self.name)

    def _trim_spaces(self):
        self.name = self.name.replace('ASSEMBL Y', 'ASSEMBLY')


class IndianaPDFTableHeaderParser(PDFStringIterator):
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
        office = None
        while True:
            s = self._get_next_string()
            if not office:
                office = Office(s)
            else:
                office.append(s)
            if office.is_valid():
                office.extract_district()
                office.normalize()
                yield office
                office = None
                for skipped_header_string in SKIPPED_HEADER_STRINGS:
                    while skipped_header_string in self._peek_next_string():
                        self._get_next_string()  # skip 'VOTE FOR X' and 'X of X Precincts Reporting' strings
                if not self._next_string_is_header_string():
                    break

    def _parse_subheaders(self, offices):
        for office in offices:
            candidate = None
            while self._more_subheaders_exist():
                s = self._get_next_string()
                if self._ignorable_string(s):
                    continue
                if not candidate:
                    candidate = s
                else:
                    candidate += ' ' + s
                if candidate in VALID_SUBHEADERS:
                    if (candidate, self._peek_next_string()) in GREEDY_SUBHEADER_PAIRS:
                        continue  # prevent greedy column parse
                    yield from self._process_candidate(candidate, office)
                    if candidate == 'Contest Total':
                        break
                    candidate = None

    def _next_string_is_header_string(self):
        s = self._peek_next_string()
        return s == 'STATISTICS' or s.startswith('DEM') or s.startswith('REP')

    def _more_subheaders_exist(self):
        if self._previous_table_header:
            if len(self._previous_table_header) <= self._strings_offset:
                return False
        return self._peek_next_string() != FIRST_ROW_PRECINCT

    @staticmethod
    def _ignorable_string(s):
        return s == '-' or s.startswith('VOTE FOR')

    @staticmethod
    def _process_candidate(candidate, office):
        if office.name == 'STATISTICS':
            candidate, party_full = candidate.split(' - ')
            party = PARTY_TO_ABBREVIATION.get(party_full, party_full)
            yield Candidate(candidate, '', party, '')
        else:
            yield Candidate(office.name, office.district, office.party, candidate)


class IndianaPDFTableBodyParser(PDFStringIterator):
    def __init__(self, strings, candidates):
        super().__init__(strings)
        self._candidates = candidates
        self._table_is_done = False

    def __iter__(self):
        while self._has_next_string() and not self._table_is_done:
            if self._peek_next_string().startswith(FIRST_FOOTER_SUBSTRING):
                break
            precinct = self._get_next_string()
            yield from self._parse_row(precinct)

    def table_is_done(self):
        return self._table_is_done

    def _parse_row(self, precinct):
        self._table_is_done = precinct == LAST_ROW_PRECINCT
        for candidate in self._candidates:
            if self._table_is_done and self._peek_next_string().startswith(FIRST_FOOTER_SUBSTRING):
                # totals row may not necessarily contain every column
                break
            votes = self._get_next_string()
            if not self._table_is_done and not self._candidate_is_invalid(candidate):
                votes = int(votes.replace(',', ''))
                yield ParsedRow(COUNTY, precinct.title(), candidate.office.title(),
                                candidate.district, candidate.party,
                                candidate.name.replace('Write-in: ', '').title(), votes)

    @staticmethod
    def _candidate_is_invalid(candidate):
        if candidate.party == 'Blank':
            return True
        if candidate.office == 'Voter Turnout' or 'DELEGATE' in candidate.office:
            return True
        return candidate.name in ('Total Votes Cast', 'Contest Total', 'Write-in Totals')


class IndianaPDFPageParser:
    def __init__(self, page, previous_table_header):
        self._active_table_header = None
        self._table_body_parser = None
        strings = page.get_strings()
        header = strings[:len(INDIANA_HEADER)]
        assert (header == INDIANA_HEADER)
        self._strings = strings[len(INDIANA_HEADER):]
        self._previous_table_header = previous_table_header

    def __iter__(self):
        while not self.page_is_done():
            table_header_parser = IndianaPDFTableHeaderParser(self._strings, self._previous_table_header)
            candidates = table_header_parser.get_candidates()
            if not candidates:
                # any page without valid candidates has no additional
                # tables and is therefore skippable
                break
            self._table_headers = table_header_parser.get_table_headers()
            self._strings = table_header_parser.get_remaining_strings()
            self._table_body_parser = IndianaPDFTableBodyParser(self._strings, candidates)
            yield from iter(self._table_body_parser)
            self._strings = self._table_body_parser.get_remaining_strings()

    def page_is_done(self):
        return self._strings[0].startswith(FIRST_FOOTER_SUBSTRING)

    def get_continued_table_header(self):
        if self._table_body_parser.table_is_done():
            return None
        return self._table_headers


def pdf_to_csv(pdf, csv_writer):
    csv_writer.writeheader()
    previous_table_header = None
    for page in pdf:
        print(f'processing page {page.get_page_number()}')
        pdf_page_parser = IndianaPDFPageParser(page, previous_table_header)
        for row in pdf_page_parser:
            csv_writer.writerow(row._asdict())
        previous_table_header = pdf_page_parser.get_continued_table_header()


if __name__ == "__main__":
    with open(OUTPUT_FILE, 'w', newline='') as f:
        pdf_to_csv(PDFPageIterator(INDIANA_FILE), csv.DictWriter(f, OUTPUT_HEADER))
