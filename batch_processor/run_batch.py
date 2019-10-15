import sys
from batch_processor import BatchProcessor


# Main method for running batch job

if __name__ == "__main__":
    
    if len(sys.argv) != 4:
        sys.stderr.write("Invalid inputs.\nUsage: spark-submit --jars <jars>
                          run_batch.py <s3configfile> <schemafile> 
                          <credentialfile> \n")
        sys.exit(-1)

    s3_configfile, schema_configfile, credentfile = sys.argv[1:4]

    processor = BatchProcessor(s3_configfile, schema_configfile, credentfile)
    processor.run()
