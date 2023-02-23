class Ingest:

    def __init__(self, spark):
        # A class level variable
        self.spark = spark

    def ingest_data(self, folder_path, file_name):
        print("Ingesting from "+file_name)
        df = self.spark.read.csv(folder_path+"/"+file_name,inferSchema=True,header=True)
        return df

