# mamonit
Log analyzer for SAS Marketing Automation (tested with 6.5)

Use cases:
- Show running campaigns using active log file.
- Extract all campaign executions.
- Campaign concurrency analysis. You can also merge multiple concurrency analyses (useful for middle tier clusters).

<pre>
Usage:
mamonit.py [-h] [--log-dir LOG_DIR] [--log-file LOG_FILE]
                  [--instance-name INSTANCE_NAME] [--output-file OUTPUT_FILE]
                  [--analysis-files ANALYSIS_FILES [ANALYSIS_FILES ...]]
                  {show-running-campaigns,concurrency-analysis,extract-campaign-executions,merge-concurrency-analysis}

positional arguments:
  {show-running-campaigns,concurrency-analysis,extract-campaign-executions,merge-concurrency-analysis}
                        action to be run

optional arguments:
  -h, --help            show this help message and exit
  --log-dir LOG_DIR     log directory
  --log-file LOG_FILE   log file
  --instance-name INSTANCE_NAME
                        SASServer6 instance name
  --output-file OUTPUT_FILE
                        output results to file in CSV format
  --analysis-files ANALYSIS_FILES [ANALYSIS_FILES ...]
                        concurrency analysis files to be merged
</pre>
