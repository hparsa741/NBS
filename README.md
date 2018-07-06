Challenge
====
Build a pipeline to store and analyze a small dataset chosen from Wikipedia's publicly available server logs (http://dumps.wikimedia.org/other/pagecounts-raw). 

Requirements
====
1.Select some combination of database/processing platforms. 
---
The suggested process consists of a relational database e.g PostgreSQL to create unique sequential ids as job_log_id and store job information and also a distributed storage e.g Hive to store page count data.  
As soon as job triggered to run, a unique id is assigned and inserted into relational database as job_log_id along with run_user, start time, end time as well as later insertion of other job statistics such as file size, byte processed, cpu consumption, run status (running, schrduled, completed, failed, etc), etc. The job log for each job will be store in `./job_logs/job_{job_log_id}_{date}.log` for debuging and future references. To log the job events, a logging module like python logger or on shell `nohup python wiki_page_count.py 2>&1 | tee ./job_logs/job_{job_log_id}_{date}.log &` can be used.  
The Hive table partioned by year, month, day and hour to ensures faster data retrieval and processing.  
The data pipline gets date and hour of page count as arguments and download the file from external repository if not downloaded and inject data into Hive table if not injected previously; download and injection can be forced re-run with an option otherwise they are skipped to ensure the process is idempotent. Then wiki page count data analysis can be performed by querying against the Hive table.  
An alternative to Hive would be binary or columnar databeses or file formats like Apache Avro, Hbase, Parquet or Kudu that are highly efficient and scalable.

2 and 3.Create and run an importation command or procedure for loading the wikipedia data set into some database for later analysis. Develop a small application in your favorite language to read from your data store
---
[This python code](https://github.com/hparsa741/NBS/blob/master/wiki_page_count.py) is a simple implementation of the suggested process in section 1. The data pipline gets date and hour of page count as arguments. The application consists of 3 parts; mentioned in the code as part 1, 2 and 3. (1) The code downloads page count data and store the file into `./raw` directory; (2) Then it's decompressed and loaded into data frame, cleaned up and stored as pickle into `./pkl` directory. (3) And finally the data is summarized and stored as csv into `./res` directory.
