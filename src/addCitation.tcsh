#!/bin/tcsh
#
# Add/replace a citation into the database
#
# Copyright: CSIRO 2024
# George Hobbs: George.Hobbs@csiro.au, Agastya Kapur: Agastya.Kapur@csiro.au

set num_args = $#
if ($num_args !~ "1") then
 echo "./addCitation.tcsh <file>"
 echo "File = bibtex output from e.g., ADS providing the citation"
 exit
endif

set inputCitation = $1
set replace = 0

cat $inputCitation

# Does it have a v1ref?
set haveV1 = `grep v1ref $inputCitation | grep -v '#' | wc -l`
if ($haveV1 =~ "1") then
 # Is it already in the database
 #
 set v1ref = `grep v1ref $inputCitation | grep -v '#' |sed s/"{"/""/g | sed s/"}"/""/g | awk '{print $3}'`
 echo $v1ref
 set result = `sqlite3 psrcat2.db "SELECT citation_id FROM citation WHERE v1label='$v1ref'" ;`
 if ($result =~ "") then
  echo "Not in database"
  set replace = 0
 else
  echo "Already in database"
  set replace = 1
  set citationID=-1
 endif

endif


set articleLabel = `grep ARTICLE $inputCitation | grep -v '#' |awk -F "{" '{print $2}' | sed s/","/""/g`
# Check if the label is already in
if ($replace =~ "0") then
 #
 # Check if this is already in the database
 #
 echo $articleLabel
 set citationID = `sqlite3 psrcat2.db "SELECT citation_id FROM citation WHERE label='$articleLabel'" ;`
 if ($citationID =~ "") then
  echo "Not in database (LABEL)"
  set replace = 0
 else
  echo "Already in database (LABEL)"
  set replace = 1

 endif

endif


set author = `grep "author =" $inputCitation | grep -v '#' | awk -F "author = " '{print $2}' | sed s/"{"/""/g | sed s/"}"/""/g | awk '{if (substr($0,length($0),1)==",") {print substr($0,1,length($0)-1)} else {print $0}}'`
set title = `grep "title =" $inputCitation | grep -v '#' | awk -F "title =" '{print $2}' | sed s/"{"/""/g | sed s/"}"/""/g | sed s/'"'/""/g |  awk '{if (substr($0,length($0),1)==",") {print substr($0,1,length($0)-1)} else {print $0}}'`
set journal = `grep "journal =" $inputCitation | grep -v '#' | awk -F "journal =" '{print $2}' | sed s/"{"/""/g | sed s/"}"/""/g | sed s/'"'/""/g | sed s/"\\"/""/g |  awk '{if (substr($0,length($0),1)==",") {print substr($0,1,length($0)-1)} else {print $0}}'`
set year = `grep "year =" $inputCitation | grep -v '#' | awk -F "year =" '{print $2}' | sed s/"{"/""/g | sed s/"}"/""/g | sed s/'"'/""/g |  awk '{if (substr($0,length($0),1)==",") {print substr($0,1,length($0)-1)} else {print $0}}'`
set month = `grep "month =" $inputCitation | grep -v '#' | awk -F "month =" '{print $2}' | sed s/"{"/""/g | sed s/"}"/""/g | sed s/'"'/""/g |  awk '{if (substr($0,length($0),1)==",") {print substr($0,1,length($0)-1)} else {print $0}}'`
set volume = `grep "volume =" $inputCitation | grep -v '#' | awk -F "volume =" '{print $2}' | sed s/"{"/""/g | sed s/"}"/""/g | sed s/'"'/""/g |  awk '{if (substr($0,length($0),1)==",") {print substr($0,1,length($0)-1)} else {print $0}}'`
set number = `grep "number =" $inputCitation | grep -v '#' | awk -F "number =" '{print $2}' | sed s/"{"/""/g | sed s/"}"/""/g | sed s/'"'/""/g |  awk '{if (substr($0,length($0),1)==",") {print substr($0,1,length($0)-1)} else {print $0}}'`
set pages = `grep "pages =" $inputCitation | grep -v '#' | awk -F "pages =" '{print $2}' | sed s/"{"/""/g | sed s/"}"/""/g | sed s/'"'/""/g |  awk '{if (substr($0,length($0),1)==",") {print substr($0,1,length($0)-1)} else {print $0}}'`
set doi = `grep "doi =" $inputCitation | grep -v '#' | awk -F "doi =" '{print $2}' | sed s/"{"/""/g | sed s/"}"/""/g | sed s/'"'/""/g |  awk '{if (substr($0,length($0),1)==",") {print substr($0,1,length($0)-1)} else {print $0}}'`

# Should also check for "url"
set url = `grep "adsurl =" $inputCitation | grep -v '#' | awk -F "adsurl =" '{print $2}' | sed s/"{"/""/g | sed s/"}"/""/g | sed s/'"'/""/g |  awk '{if (substr($0,length($0),1)==",") {print substr($0,1,length($0)-1)} else {print $0}}'`

# Check spaces between v1ref and =
set v1ref = `grep "v1ref =" $inputCitation | grep -v '#' | awk -F "v1ref =" '{print $2}' | sed s/"{"/""/g | sed s/"}"/""/g | sed s/'"'/""/g |  awk '{if (substr($0,length($0),1)==",") {print substr($0,1,length($0)-1)} else {print $0}}'`

echo "Label: $articleLabel"
echo "Author: $author"
echo "Title: $title"
echo "Journal: $journal"
echo "Year: $year"
echo "Month: $month"
echo "Volume: $volume"
echo "Number: $number"
echo "Pages: $pages"
echo "DOI: $doi"
echo "URL: $url"
echo "V1ref: $v1ref"

### Now enter into the database
# Do we need to replace or add?

set versionID = `sqlite3 psrcat2.db "SELECT version_id FROM VERSION ORDER BY version_id DESC LIMIT 1;"`

if ($replace =~ "1") then
 if ($citationID =~ "-1") then
  set citationID = `sqlite3 psrcat2.db "SELECT citation_id FROM citation WHERE v1label='$v1ref';" `
 endif
 echo "Citation $inputCitation exists in DB already"
 echo "$citationID"

 # Need to replace this citationID with the new information
 sqlite3 psrcat2.db "UPDATE citation SET label='$articleLabel',title='$title',author='$author',journal='$journal',year='$year',month='$month',volume='$volume',number='$number',pages='$pages',doi='$doi',url='$url',version_id='$versionID' WHERE citation_id=$citationID"
else

 sqlite3 psrcat2.db "INSERT INTO citation(citation_id,v1label,label,title,author,journal,year,month,volume,number,pages,doi,url,version_id) VALUES (NULL,'$v1ref','$articleLabel','$title','$author','$journal','$year','$month','$volume','$number','$pages','$doi','$url','$versionID'); SELECT last_insert_rowid();"

endif


