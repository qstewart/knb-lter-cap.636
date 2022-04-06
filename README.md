## knb-lter-cap.636

### CAP LTER 10-m towers at Lost Dutchman State Park and the Desert Botanical Garden

This repository features the workflow for publishing the 10-m tower data, documentation relating to tower equipment and operation, and a log of equipment maintenance (mostly sensor calibration or replacement).

- data processing and upload: the workflow for processing and uploading tower data to the database has been moved from this repository to the [capmicromet](https://gitlab.com/caplter/capmicromet) R package.
- database details are in the [capmicromet-database](https://gitlab.com/caplter/capmicromet-database) repository
- CAP LTER technicians should please annotate the `maintenance_log.csv` file in this repository with all maintenance activities relating to this project

#### knb-lter-cap.636.10

- split observations into approximately decadal intervals to address the large and increasingly unwieldy file size resulting from encapsulating all data into a single file
- added disturbance to keyword list

#### knb-lter-cap.636.9

- data refresh
- as part of this refresh, we noticed that the logger at the DBG site is not recording data in the evening hours
