# Copy stable version (psrcat2.0.1.db) to a new 'temporary name' psrcat2.0.2.db
echo "Copy version to temporary name"
cp psrcat2.0.1.db psrcat2.0.2.db

# Create temporary table with added version_id and foreign key constraint.
# Making new table because SQLITE doesn't allow to add foreign key constraint with ALTER TABLE
echo "Create newcitation table"
sqlite3 psrcat2.0.2.db "CREATE TABLE newcitation ( \
 citation_id integer PRIMARY KEY, \
 v1label text, \
 label text, \
 title text, \
 author text, \
 journal text, \
 year text, \
 month text, \
 volume text, \
 number text, \
 pages text, \
 doi text, \
 url text, \
 version_id integer, \
 foreign key (version_id) REFERENCES version (version_id), \
 UNIQUE(label,title,author,journal,year,month,volume,number,pages,doi,url) \
);"

# Copy the citations into the new table
echo "Copy citations to new table"
sqlite3 psrcat2.0.2.db "INSERT into newcitation (citation_id,v1label,label,title,author,journal,year,month,volume,number,pages,doi,url) SELECT citation_id,v1label,label,title,author,journal,year,month,volume,number,pages,doi,url from citation"

# Remove citation table
echo "Remove old citation table"
sqlite3 psrcat2.0.2.db "DROP TABLE citation"

# Rename newcitation table to citation
echo "Rename newcitation to citation table"
sqlite3 psrcat2.0.2.db "ALTER TABLE newcitation RENAME TO citation"

# Insert version id
echo "Insert version information"
sqlite3 psrcat2.0.2.db "INSERT INTO version (version_id,version,entryDate,notes) VALUES (NULL,'2.1.0',DATETIME('now'),'This has primarily been produced by George Hobbs, Agastya Kapur and Lawrency Toomey with support from Dick Manchester. \
 \
Software versions: Made by running .version in sqlite3\
SQLite 3.41.2 2023-03-22 11:56:21 0d1fc92f94cb6b76bffe3ec34d69cffde2924203304e8ffc4155597af0c191da \
zlib version 1.2.13 \
gcc-11.2.0 \
 \
Software collection DOI: \
 \
Git Commit Tag: \
 \
Update file labels: \
bgf+23,ccp+23,djk+23,gvf+23,lpz+23,sbb+23,wyw+23,crafts23' \
)"

# Change values of timeDerivative of FLUX and WIDTH to 0 from NULL
echo "Change timederiv values of FLUX and WIDTH"
sqlite3 psrcat2.0.2.db "UPDATE parameter SET timeDerivative=0 WHERE parameterType_id=16"
sqlite3 psrcat2.0.2.db "UPDATE parameter SET timeDerivative=0 WHERE parameterType_id=17"

# Insert tagToCitation
echo "Create table tagToCitation"
sqlite3 psrcat2.0.2.db "CREATE TABLE IF NOT EXISTS tagToCitation ( \
 tagToCitation_id integer PRIMARY KEY, \
 citation_id integer, \
 tag_id integer, \
 foreign key (tag_id) REFERENCES tag (tag_id), \
 foreign key (citation_id) REFERENCES citation (citation_id) \
)"
# Copy this version with updated timeDerivatives to psrcat2.db

# Delete duplicate PMRA,PMDEC parameters for PSR J2047+1053
echo "Delete duplicate parameters"
sqlite3 psrcat2.0.2.db "DELETE FROM parameter where (parameter_id IN (32405,32406,32407,32408,32409,32410) AND pulsar_id=3274);"

# Insert viewParameter VIEW into the database
echo "Inserting viewParameter VIEW"
sqlite3 psrcat2.0.2.db < viewParameter.sql

# Copy version with all 'fixes'
echo "Copied versions"
cp psrcat2.0.2.db psrcat2.db

# Remove fixed version since a copy has made
\rm psrcat2.0.2.db

# Input the following papers into the database. Commit 0 means the data will not be commited. Commit 1 will input
./inputUpdate.tcsh -upd crafts23.txt -commit 1
./inputUpdate.tcsh -upd bgf+23.txt -commit 1
./inputUpdate.tcsh -upd djk+23.txt -commit 1
./inputUpdate.tcsh -upd gvf+23.txt -commit 1
./inputUpdate.tcsh -upd ccp+23.txt -commit 1
./inputUpdate.tcsh -upd sbb+23.txt -commit 1
./inputUpdate.tcsh -upd lpz+23.txt -commit 1
./inputUpdate.tcsh -upd wyw+23.txt -commit 1

# Derive parameters. Currently derives DM_YMW16 and DM_NE2001
echo "Deriving parameters now"
python deriveParameters.py

# Convert V2 to V1
echo "Converting to psrcat version 1"
./psrcatV2_V1 2.1.0 psrcat2.db > psrcat.db

