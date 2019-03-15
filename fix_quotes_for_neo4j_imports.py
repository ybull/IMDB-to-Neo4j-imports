"""
Created to use with our IS490DA class.
March 2019

Neo4j's LOAD CSV command is apparently incapable of ignoring quotes, but some
delimited files (such as those from IMDB) contain un-escaped quotes within
data fields that are not enclosed wrapped with quote delimiters or escaped
within.

The only fix for this is to rewrite the files so that all fields containing any
quotes get wrapped in quotes and also have existing quote characters
doubled.

"""

import re
import os.path

i = input('Enter the filename to convert :')
if not os.path.exists(i):
    print('Input file not found. Exiting.')
    exit()

default_outfile_name = "fixed." + i
print('Output default:', default_outfile_name)
o = input('Hit Return to use default, or enter a different filename to create:')
if o.strip() == '':
    o = default_outfile_name

if os.path.exists(o):
    print('Warning. output file already exists.')
    response = input('Enter an O to confirm overwriting, otherwise this will quit.')
    if response not in ['o', 'O']:
        exit()

outfile = open(o, 'w', encoding='utf8')

count = 0
with open(i, 'r', encoding='utf8') as infile:
    for line in infile:
        count += 1
        if '"' in line:
            # this line has double quotes.
            was = line  # copy for comparison later

            fields = line.split('\t')
            for i in range(len(fields)):
                f = fields[i]
                if '"' not in f:
                    continue  # no quotes in this field

                if re.match(r'\A"[^"]*"\Z', f):  # quotes ONLY appear at start & end
                    continue  # no problem here.

                # IMDB has a field with a LIST of quoted strings. Leave as-is:
                if re.match(r'\[".*"\]', f):
                    continue  # no problem here.

                # First, we change every existing " char into a pair ""
                f = f.replace('"', '""')

                # Then, we wrap the whole field in outer pair of quotes:
                f = '"' + f + '"'
                fields[i] = f

            line = '\t'.join(fields)   # reconstruct the whole line

            # if we are modifying the line, print before and after so we can
            #  see exactly what is changing:
            if was != line:
                print('line {:9} was:'.format(count),
                      was, end='')
                print('line {:9} now:'.format(count),
                      line)

        print(line, file=outfile, end='')

