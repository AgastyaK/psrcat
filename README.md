# The ATNF Pulsar Catalogue

Code and data files for maintaining a catalogue of pulsar parameters

## Description

PSRCAT is the result of regular searches of the literature
over a period of many years, distilling the data down to
the "best" current representation of what is known about
each pulsar. The best description is in Manchester et. al, (2005).

Version 2 of the catalogue (hereafter PSRCAT2) is under development 
and is entirely database driven allowing for more flexible use cases. 
Currently, PSRCAT2 output is converted to the traditional 
(hereafter v1) output (a text file, psrcat.db) for backward compatibility.

The catalogue can be accessed via a web interface
(https://www.atnf.csiro.au/research/pulsar/psrcat/), where a user will also find
instructions on downloading and installing the v1 source code which
can be used to query the v1 or PSRCAT2 database.

## Getting Started

### Dependencies

The C code should compile on any UNIX-like system.

The web interface requires a web server with support for php 7 or later.

### Installing

You should be able to run the compiled program in the unpacked code directory.
See src/README for the (simple) details.
Relocate the binary and catalogue files to any convenient location.

### Executing program

* The C program has a multitude of arguments that query or effect changes
  to the catalogue files. e.g.

```
psrcat -db_file psrcat.db
```

## Help

```
psrcat -h
```

## Authors

* Dick Manchester
* George Hobbs
* Lawrence Toomey
* Albert Teoh
* Agastya Kapur
* and many others

## Version History

PSRCAT was developed using CVS for revision control from 2003 - 2024.
The complete history is preserved in the 'psrcat1' branch of this repository.

A primitive version of the web interface appears in the web/ subdirectory,
along with some Java code for plotting. In 2015, the web interface was forked
to allow for easier development:

    https://bitbucket.csiro.au/scm/casssoft/psrcat-web.git

The fork imported the code as it existed on the www.atnf.csiro.au website,
which had changed significantly since the last CVS revision to any of the
php code (2013-07-04). Since then there has been a small divergence,
limited to the following files:
* web/versions.txt
* web/psrcat\_help.html


In 2023, work started on PSRCAT2.


## Licence

This project is licensed under the GNU Public License, version 3 or later.

* src/wc\_strncmp.c
  is Copyright 1992-2002 by Pete Wilson (pete@pwilson.net)
* src/evaldefs.h, evalkern.c, evalkern.syn, evalwrap.c
  are all Copyright (c) 1996 - 1999 Parsifal Software


## Acknowledgments

## References

"The ATNF Pulsar Catalogue", R. N. Manchester, G. B. Hobbs, A. Teoh & M. Hobbs, Astronomical Journal, 129, 1993-2006 (2005)

