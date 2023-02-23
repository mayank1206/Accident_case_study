import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct,row_number,col,regexp_extract
import ingest
import transform
import persist

class Pipeline:
    def create_spark_session(self):
        # A class level variable
        self.spark = SparkSession.builder \
            .appName("BCG Case Study") \
            .getOrCreate()

    def run_pipeline(self, folder_path, destination_path):
        print("Running Pipeline")
        ingest_process = ingest.Ingest(self.spark)
        tranform_process = transform.Transform(self.spark)
        persist_process = persist.Persist(self.spark)

        person_df = ingest_process.ingest_data(folder_path, "Primary_Person_use.csv")
        damages_df = ingest_process.ingest_data(folder_path, "Damages_use.csv")
        charges_df = ingest_process.ingest_data(folder_path, "Charges_use.csv")
        units_df = ingest_process.ingest_data(folder_path, "Units_use.csv")

        units_df = tranform_process.remove_dublicates(units_df)

        assignment1_df = tranform_process.assignment1(person_df)
        persist_process.persist_data(assignment1_df, destination_path,"assignment_1.csv")
       
        assignment2_df = tranform_process.assignment2(units_df)
        persist_process.persist_data(assignment2_df, destination_path,"assignment_2.csv")
       
        assignment3_df = tranform_process.assignment3(person_df)
        persist_process.persist_data(assignment3_df, destination_path,"assignment_3.csv")
       
        assignment4_df = tranform_process.assignment4(person_df)
        persist_process.persist_data(assignment4_df, destination_path,"assignment_4.csv")
       

        assignment5_df = tranform_process.assignment5(person_df,units_df)
        persist_process.persist_data(assignment5_df, destination_path,"assignment_5.csv")

        assignment6_df = tranform_process.assignment6(person_df,units_df)
        persist_process.persist_data(assignment6_df, destination_path,"assignment_6.csv")

        assignment7_df = tranform_process.assignment7(units_df,damages_df)
        persist_process.persist_data(assignment7_df, destination_path,"assignment_7.csv")

        assignment8_df = tranform_process.assignment8(person_df, units_df, charges_df)
        persist_process.persist_data(assignment8_df, destination_path,"assignment_8.csv")
    

if __name__ == '__main__':
    try:
        folder_path=sys.argv[1]
        destination_path=sys.argv[2]
        pipeline = Pipeline()
        pipeline.create_spark_session()
        pipeline.run_pipeline(folder_path, destination_path)
    except:
        print("logging the error")
    

