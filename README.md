# knb-lter-cap.636
## CAP LTER 10m towers at Lost Dutchman State Park and the Desert Botanical Garden

This repository contains the R code for publishing the 10m tower data.

In Jan 2018, the repository was expanded to include an R workflow for uploading tower data to the database after the Ruby script(s) that had served this purpose were no longer functional (or, at least, practical). Note that the workflow adds data to the database in its current configuration, which is as a wide format and includes separate tables for LDP and DBG data. This needs to be rectified in the future such that the DB structure is a more appropriate long format, and the upload scripts will have to be modified accordingly. 

The repository also now houses, for posterity, the tower operating manual and loggernet programs that are, as much as I am aware, the programs currently employed in the towers. The previous repository that housed the upload functionality (ssh://git.aws.gios.asu.edu/common/git/lter120tower-ruby), was an archive for raw data downloads, and housed the loggernet programs, operations manual, etc. is generally deprecated (but still exists in case information not copied over is ever needed).

Raw data download are now house in Dropbox (ASU::Loggernet downloads).
