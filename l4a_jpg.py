from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import schedule
import time
import gridfs
import numpy as np
import matplotlib.pyplot as plt
from PIL import Image
import io
from bson import ObjectId
import pickle
import logging
import gc

def connect_MongoDB(client, db_name, collection):
    
    client = MongoClient(client)
    user_db = client[db_name]
    user_collection = user_db[collection]
    user_fs = gridfs.GridFS(user_db, collection=collection)
    
    return user_collection, user_fs

class ETL:
    
    def __init__(self, collection_jpg, fs_jpg):

        # 連結資料庫
        self.collection, self.fs = connect_MongoDB(client='mongodb://ivan:ivan@10.88.26.183:27017', db_name="AT", collection="L4A_charge2d")
        self.collection_jpg = collection_jpg
        self.fs_jpg = fs_jpg
        
    def etl(self):
        
        df = pd.DataFrame.from_records(self.collection.find({'lm_time': {'$gte': (datetime.now()-timedelta(days=150)).strftime("%Y/%m/%d")}}))

        if df.empty:
            
            print("時間內無資料")
            
        else:
                    
            chip_id_lst = df["chip_id"].unique()

            for chip_id in chip_id_lst:
                
                logging.info(chip_id)
                print(chip_id)

                df_chip = df[df["chip_id"]==chip_id]
                op_lst = df_chip["op_id"].unique()

                for op_id in op_lst:

                    df_op = df_chip[df_chip["op_id"]==op_id]
                    step_lst = df_op["step"].unique()        

                    for step in step_lst:

                        df_step = df_op[df_op["step"]==step]
                        ins_cnt_lst = df_step["ins_cnt"].unique()

                        for ins_cnt in ins_cnt_lst:

                            df_ins_cnt = df_step[df_step["ins_cnt"]==ins_cnt]
                            charge_type_lst = df_ins_cnt["charge_type"].unique()            

                            for charge_type in charge_type_lst:

                                df_charge_type = df_ins_cnt[df_ins_cnt["charge_type"]==charge_type]
                                df_charge_type = df_charge_type.reset_index(drop=True)                                      

                                left, right, bottom, top, wspace, hspace = 0.1, 0.91, 0.07, 0.95, 0, 0
                                figsize_rgb = (14, 10)
                                figsize_w = (12, 3)  
                                
                                product = df_charge_type["recipe_id"][0][:4]
                                
                                if product == "Y136":
                                    W = 1440
                                    H = 270
                                elif product == "Y173":
                                    W = 3840
                                    H = 720
                                elif product in ["V160","V161"]:
                                    W = 720
                                    H = 540   
                                elif product == "Z300":
                                    W = 2070
                                    H = 156         
                                elif product == "Z123":
                                    W = 4800
                                    H = 600                                                

                                self.plot_sheet(df_charge_type, W, H,
                                                chip_id, op_id, step, ins_cnt, charge_type,
                                                left, right, bottom, top, wspace, hspace,
                                                figsize_rgb, figsize_w
                                                )

    def plot_sheet(self, df, W, H,
                    chip_id, op_id, step, ins_cnt, charge_type,
                    left, right, bottom, top, wspace, hspace,
                    figsize_rgb, figsize_w
                    ):
        
        # config
        sheet_2d_object_id_lst = []
        color_dict = {"Reds":"2d_r_object_id",
                    "Greens":"2d_g_object_id",
                    "Blues":"2d_b_object_id"}
        
        for color in ["Reds","Greens","Blues"]:
            
            plt.close('all')
            fig, ax = plt.subplots(figsize=figsize_rgb)
            plt.subplots_adjust(left=left, bottom=bottom, right=right, top=top, wspace=wspace, hspace=hspace)
            
            arr = self.fs.get(ObjectId(df[color_dict[color]][0])).read()
            arr = pickle.loads(arr)
            
            plt.axis('on')
            ax.imshow(arr, cmap=color)
            ax.set_xticks([])
            ax.set_yticks([])
            ax.set_aspect('equal')

            plt.savefig('l4a_temp.jpg')
            image = Image.open("l4a_temp.jpg")
            image_bytes = io.BytesIO()
            image.save(image_bytes, format="JPEG")
            image_bytes.seek(0)

            sheet_2d_object_id_lst.append(self.fs_jpg.put(image_bytes, filename="l4a_temp.jpg"))

        plt.close('all')
        fig, ax = plt.subplots(figsize=figsize_w)
        plt.subplots_adjust(left=left, bottom=bottom, right=right, top=top, wspace=wspace, hspace=hspace)
        
        charge_2d_r = pickle.loads(self.fs.get(ObjectId(df["2d_r_object_id"][0])).read())
        charge_2d_g = pickle.loads(self.fs.get(ObjectId(df["2d_g_object_id"][0])).read())
        charge_2d_b = pickle.loads(self.fs.get(ObjectId(df["2d_b_object_id"][0])).read())

        charge_2d = np.concatenate((charge_2d_r.flatten(), 
                                    charge_2d_g.flatten(), 
                                    charge_2d_b.flatten()))

        charge_2d_ori = np.zeros(W*H)

        charge_2d_ori[0::3] = charge_2d[:W*H//3]
        charge_2d_ori[1::3] = charge_2d[W*H//3:2*W*H//3]
        charge_2d_ori[2::3] = charge_2d[2*W*H//3:]  

        charge_2d_ori = np.reshape(charge_2d_ori,(H,W))  
        
        plt.axis('on')
        ax.imshow(charge_2d_ori, cmap="Greys")
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_aspect('equal')

        plt.savefig('l4a_temp.jpg')
        image = Image.open("l4a_temp.jpg")
        image_bytes = io.BytesIO()
        image.save(image_bytes, format="JPEG")
        image_bytes.seek(0)
        
        sheet_2d_object_id_lst.append(self.fs_jpg.put(image_bytes, filename="l4a_temp.jpg"))
        
        table_schema = {'lm_time': df["lm_time"].unique()[0],
                        'eqp_id': df["eqp_id"].unique()[0],
                        'op_id': op_id,
                        'recipe_id': df["recipe_id"].unique()[0],
                        'chip_id': chip_id,
                        'ins_cnt': ins_cnt,
                        'step': step,
                        'charge_type': charge_type,
                        '2d_r_object_id': sheet_2d_object_id_lst[0],
                        '2d_g_object_id': sheet_2d_object_id_lst[1],
                        '2d_b_object_id': sheet_2d_object_id_lst[2],
                        '2d_w_object_id': sheet_2d_object_id_lst[3]
                        }
        try:
            self.collection_jpg.insert_one(table_schema)
            del table_schema
            gc.collect()
        except:
            # db 內本來就有資料
            pass   

def job():

    logging.basicConfig(filename=f'log/l4a_jpg_{datetime.today().strftime("%Y_%m_%d_%H_%M")}.log', filemode='w+', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    # 選擇 client
    client = MongoClient('mongodb://ivan:ivan@10.88.26.183:27017')
    # 選擇 Database
    db = client["AT_jpg"]
    # 選擇 collection
    collection_jpg = db["L4A_JPG"]
    # 創建 index 避免資料重複塞進資料庫
    collection_jpg.create_index([("lm_time", 1),
                                ("eqp_id", 1),
                                ("op_id", 1),
                                ("recipe_id", 1),
                                ("chip_id", 1),
                                ("ins_cnt", 1),
                                ("step", 1),
                                ("charge_type", 1)], 
                            unique=True)    
    # 選擇 gridfs
    fs_jpg = gridfs.GridFS(db, collection="L4A_JPG")    
        
    logging.info("instantiate object")
    print("instantiate object")
    etl_obj = ETL(collection_jpg, fs_jpg)
    
    print("The current date and time is", datetime.now().strftime("%d/%m/%Y, %H:%M:%S"))

    logging.info("ETL L4A FS JPG")
    print("ETL L4A FS JPG")  
    etl_obj.etl()
    
    logging.info("==========Done==========")
    print("==========Done==========")

if __name__ == '__main__':

    # 啟動 Job
    job()
    # schedule.every(12).hours.do(job)

    while True:  
        schedule.run_pending()    
        time.sleep(1)
