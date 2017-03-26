# This is the folder for data from 2016.
Because of the Massachusetts [Department of Public Utilities](http://www.mass.gov/eea/grants-and-tech-assistance/guidance-technical-assistance/agencies-and-divisions/dpu/) release schedule, this data would have been released in early 2017.

----
## How is this Directory Structured?
### 0. Origin
This folder contains data that was downloaded from the department of public utilities. No editing (aside from changing filenames, where necessary) has been done.

### 1. HEETMA Extract
These are the .`xls`, `.xlxs`, and `.csv` files that [HEETMA](http://www.heetma.org/) prepared for the project. Data was extracted from the `0. Origin` folder.

### 2. Pre-Process
These are `.csv` files being prepared for upload to a geocoding API. They are edited versions of those files in `1. HEETMA Extract`

### 3. JSON
This contains `.json` responses from the geocoding api, and scripts for using the gecoding api. Subdirectories and files are named according to the utility company and ID's of leaks and repairs.
