from pyspark.sql import Window
from pyspark.sql.functions import countDistinct,row_number,col,regexp_extract,sum as _sum
class Transform:
    def __init__(self, spark):
        self.spark = spark

    def remove_dublicates(self, units_df):
        """ According to my understanding there should be no dublicates of ("CRASH_ID","UNIT_NBR") in Unit data
            Below code will remove the dublicates and I order it by VIN because it is an important so it should not be null """
        print("Transforming")
        w = Window.partitionBy(["CRASH_ID","UNIT_NBR"]).orderBy(col("VIN").desc())
        units_df = units_df.withColumn("rn",row_number().over(w))\
                           .filter(col("rn") == 1) \
                           .drop("rn")
        return units_df


    def assignment1(self, person_df):
        """
        The number of crashes in which number of persons killed are male
        """
        dead_count_df = person_df.filter(col("PRSN_GNDR_ID") == "MALE").agg(_sum("DEATH_CNT").alias("TOTAL_MALE_DEATH"))
        return dead_count_df

    def assignment2(self, units_df):
        """
        The number of two wheelers are booked for crashes
        
        Some Assumptions:
        we have multiple crash with same vehicle, So I only count them once 
        Because there is some crashes where we don't have VIN, So I didn't count them
        """
        print("Assignment 2")

        two_wheeler_df = units_df.filter((col("VEH_BODY_STYL_ID") == "MOTORCYCLE") | (col("VEH_BODY_STYL_ID") == "POLICE MOTORCYCLE")) \
                                 .filter(col("VIN").isNotNull()) \
                                 .select(countDistinct("VIN"))
        return two_wheeler_df

    def assignment3(self, person_df):
        """
        State which has highest number of accidents in which females are involved
        
        Some Assumptions:
        Not able to find any columns where crash happened So I solved below Query
        Which state has highest number issued driving licence to the females which are involved in accident? 
        """
        print("Assignment 3")

        female_df = person_df.filter(col("PRSN_GNDR_ID") == "FEMALE") \
                             .groupBy("DRVR_LIC_STATE_ID").count() \
                             .orderBy(col("count").desc()) \
                             .limit(1) \
                             .drop("count")
        return female_df
    
    def assignment4(self, units_df):
        """
        Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
        """
        print("Assignment 4")

        w = Window.orderBy(col("INJRY_DEATH_CNT").desc())
        df_injry_and_death_cnt = units_df.withColumn("INJRY_AND_DEATH_CNT",col("TOT_INJRY_CNT")+col("DEATH_CNT")) \
                                .groupBy("VEH_MAKE_ID").agg(_sum("INJRY_AND_DEATH_CNT").alias("INJRY_DEATH_CNT")) \
                                .withColumn("rn",row_number().over(w)) \
                                .filter((col("rn") >=5) & (col("rn") <=15)) \
                                .select("VEH_MAKE_ID")
        return df_injry_and_death_cnt

    def assignment5(self, person_df, units_df):
        """
        All the body styles involved in crashes, mention the top ethnic user group of each unique body style  
        
        Some Assumptions:
        Because Question state "user group" so I am filtering on driver
        """
        print("Assignment 5")

        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc()) 
        driver_person_df = person_df.filter(col("PRSN_TYPE_ID") == "DRIVER")
        df_body_eth = units_df.join(driver_person_df,(units_df.CRASH_ID ==  driver_person_df.CRASH_ID) & (units_df.UNIT_NBR ==  driver_person_df.UNIT_NBR) ,"inner") \
                              .groupBy(["VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID"]).count() \
                              .withColumn("rn",row_number().over(w)) \
                              .where(col("rn") == 1) \
                              .drop("rn") \
                              .drop("count")    
        return df_body_eth
    
    def assignment6(self, person_df, units_df):
        """
        The Top 5 Zip Codes with highest number car crashes with alcohols as the contributing factor to a crash  
        """
        print("Assignment 6")

        person_zip_df = person_df.filter(col("DRVR_ZIP").isNotNull())
        df_car_alcohol = units_df.filter((col("VEH_BODY_STYL_ID").contains("CAR")) & ((col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")) | (col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")))) 
        df_zip  = df_car_alcohol.join(person_zip_df,(df_car_alcohol.CRASH_ID ==  person_zip_df.CRASH_ID) & (df_car_alcohol.UNIT_NBR ==  person_zip_df.UNIT_NBR) ,"inner") \
                        .groupBy("DRVR_ZIP").count() \
                        .orderBy(col("count").desc()) \
                        .limit(5) \
                        .drop("count")    
        return df_zip

    def assignment7(self, units_df,damages_df):
        """
        Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        """
        print("Assignment 7")

        df_car_insurance = units_df.filter((col("VEH_BODY_STYL_ID").contains("CAR")) & (col("FIN_RESP_TYPE_ID").contains("INSURANCE")))
        damages_df = damages_df.withColumnRenamed("CRASH_ID","DAMAGED_CRASH_ID")
        units_damages_df = df_car_insurance.join(damages_df,df_car_insurance.CRASH_ID ==  damages_df.DAMAGED_CRASH_ID,"left") \
                                    .filter((col("DAMAGED_PROPERTY").isNull()) | (col("DAMAGED_PROPERTY") == "NONE" )) \
                                    .withColumn("DAMANGE_LEVEL_1", regexp_extract("VEH_DMAG_SCL_1_ID", r'(\d+)' , 1 ).cast('bigint')) \
                                    .withColumn("DAMANGE_LEVEL_2", regexp_extract("VEH_DMAG_SCL_2_ID", r'(\d+)' , 1 ).cast('bigint')) \
                                    .filter((col("DAMANGE_LEVEL_1") > 4) | (col("DAMANGE_LEVEL_2") > 4)) \
                                    .select(countDistinct(col("CRASH_ID")).alias("DISTINCT_CRASH_ID_COUNT"))
        return units_damages_df


    def assignment8(self, person_df, units_df, charges_df):
        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
        """
        print("Assignment 8")

        charges_df_filtered = charges_df.filter(col("CHARGE").contains("SPEED"))
        df_pearson_charged = person_df.join(charges_df_filtered,(person_df.CRASH_ID ==  charges_df_filtered.CRASH_ID)
                                        & (person_df.UNIT_NBR ==  charges_df_filtered.UNIT_NBR) 
                                        & (person_df.PRSN_NBR ==  charges_df_filtered.PRSN_NBR)
                                        ,"inner")
        df_vehicle = units_df.join(df_pearson_charged,(units_df.CRASH_ID ==  person_df.CRASH_ID) 
                                & (units_df.UNIT_NBR ==  person_df.UNIT_NBR) 
                                ,"inner") 
        top_10_color = df_vehicle.groupBy(["VEH_COLOR_ID"]).count().orderBy(col("count").desc()).limit(10).select("VEH_COLOR_ID").collect()
        top_10_color_list = [data[0] for data in top_10_color]
        top_25_state = df_vehicle.groupBy(["VEH_LIC_STATE_ID"]).count().orderBy(col("count").desc()).limit(25).select(["VEH_LIC_STATE_ID"]).collect()
        top_25_state_list = [data[0] for data in top_25_state]
        df_vehicle = df_vehicle.filter((col("VEH_COLOR_ID").isin(top_10_color_list)) & (col("VEH_LIC_STATE_ID").isin(top_25_state_list))) \
                                .groupBy(["VEH_MAKE_ID"]).count() \
                                .orderBy(col("count").desc()) \
                                .limit(5) \
                                .select(col("VEH_MAKE_ID").alias("Top 5 Vehicle"))
        return df_vehicle

