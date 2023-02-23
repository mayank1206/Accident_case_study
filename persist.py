class Persist:
    def __init__(self, spark):
        self.spark = spark

    def persist_data(self, df, folder_path, file_name):
        print("Persisiting " + file_name)
        df.coalesce(1).write.csv(folder_path+"/"+file_name)    
