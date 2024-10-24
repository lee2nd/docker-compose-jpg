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

client = MongoClient('mongodb://ivan:ivan@10.88.26.183:27017')
db = client["AT_config"]
collection = db["config"]
cursor = collection.find()
df_config = pd.DataFrame.from_records(cursor).drop(columns=["_id"])

def connect_MongoDB(client, db_name, collection):
    
    client = MongoClient(client)
    user_db = client[db_name]
    user_collection = user_db[collection]
    user_fs = gridfs.GridFS(user_db, collection=collection)
    
    return user_collection, user_fs

class ETL:
    
    def __init__(self, collection_jpg, fs_jpg):

        # 連結資料庫
        self.collection, self.fs = connect_MongoDB(client='mongodb://ivan:ivan@10.88.26.183:27017', db_name="AT", collection="L6K_charge2d")
        self.collection_jpg = collection_jpg
        self.fs_jpg = fs_jpg
                
    def etl(self):
        
        df = pd.DataFrame.from_records(self.collection.find({'sheet_start_time': {'$gte': (datetime.now()-timedelta(days=150)).strftime("%Y/%m/%d")}}))
        
        if df.empty:
            
            print("時間內無資料")
            
        else:
            
            sheet_id_lst = df["sheet_id"].unique()

            for sheet_id in sheet_id_lst:
                
                logging.info(sheet_id)
                print(sheet_id)

                df_sheet = df[df["sheet_id"]==sheet_id]
                op_lst = df_sheet["op_id"].unique()

                for op_id in op_lst:

                    df_op = df_sheet[df_sheet["op_id"]==op_id]
                    sheet_time_lst = df_op["sheet_start_time"].unique()

                    for sheet_time in sheet_time_lst:

                        df_sheet_time = df_op[df_op["sheet_start_time"]==sheet_time]
                        step_lst = df_sheet_time["step"].unique()        
                                    
                        for step in step_lst:

                            df_step = df_sheet_time[df_sheet_time["step"]==step]
                            charge_type_lst = df_step["charge_type"].unique()

                            for charge_type in charge_type_lst:

                                df_charge_type = df_step[df_step["charge_type"]==charge_type]
                                df_charge_type = df_charge_type.sort_values('chip_pos')
                                df_charge_type = df_charge_type.reset_index(drop=True)  

                                X = df_config[df_config["model"]==sheet_id[:2]]["X"].values[0]
                                Y = df_config[df_config["model"]==sheet_id[:2]]["Y"].values[0]
                                W = df_config[df_config["model"]==sheet_id[:2]]["W"].values[0]
                                H = df_config[df_config["model"]==sheet_id[:2]]["H"].values[0]                                
                                                            
                                # EK/EJ
                                if (sheet_id[:2] in ["EJ","EK"]):
                                    
                                    # 大板長寬
                                    left, right, bottom, top, wspace, hspace = 0.1, 0.88, 0.12, 0.95, 0, 0
                                    figsize_rgb = (4, 4.3)
                                    figsize_w = (4, 1.4)
                                
                                # EE/EG
                                elif (sheet_id[:2] in ["EE","EG"]):
                                    
                                    # 大板長寬
                                    left, right, bottom, top, wspace, hspace = 0.1, 0.91, 0.07, 0.95, 0, 0
                                    figsize_rgb = (14, 10)
                                    figsize_w = (12, 3)

                                # EM/EL
                                elif (sheet_id[:2] in ["EM","EL"]):
                                    
                                    # 大板長寬
                                    left, right, bottom, top, wspace, hspace = 0.1, 0.9, 0.08, 0.95, 0, 0
                                    figsize_rgb = (6, 5)
                                    figsize_w = (4, 10)            
                                
                                else:
                                    continue
                                    
                                self.plot_sheet(df_charge_type, X, Y, H, W,
                                                sheet_id, op_id, sheet_time, step, charge_type,
                                                left, right, bottom, top, wspace, hspace,
                                                figsize_rgb, figsize_w
                                                )

    def plot_sheet(self, df, X, Y, H, W,
                    sheet_id, op_id, sheet_time, step, charge_type,
                    left, right, bottom, top, wspace, hspace,
                    figsize_rgb, figsize_w):
        
        # config
        sheet_2d_object_id_lst = []
        color_dict = {"Reds":"2d_r_object_id",
                    "Greens":"2d_g_object_id",
                    "Blues":"2d_b_object_id"}
        
        for color in ["Reds","Greens","Blues"]:
            
            plt.close('all')
            fig, axs = plt.subplots(Y, X, figsize=figsize_rgb)
            plt.subplots_adjust(left=left, bottom=bottom, right=right, top=top, wspace=wspace, hspace=hspace)
            
            for i in range(len(df)):

                # 先把 chargemap 2d array 抓出來
                arr = self.fs.get(ObjectId(df[color_dict[color]][i])).read()
                arr = pickle.loads(arr)
                
                if (sheet_id[:2] in ["EM","EL"]):
                    arr = np.rot90(arr, k=-1)
                
                x = Y - 1 - (ord(df['chip_pos'][i][-1]) - ord('A'))
                y = ord(df['chip_pos'][i][-2]) - ord('A')                
                
                plt.axis('on')
                axs[x, y].imshow(arr, cmap=color)
                axs[x, y].set_aspect('equal')

            # 清理所有子圖的 x 和 y ticks
            for ax in axs.flat:
                ax.set_xticks([])
                ax.set_yticks([])
                
            if (X,Y) == (2,2):

                # 在底部加入 AB
                for i, letter in enumerate(['A', 'B']):
                    fig.text((i+0.75)/2.5, 0.05, letter, ha='center', fontsize=14, weight='bold')

                # 在右側加入 AB
                for i, letter in enumerate(['A', 'B']):
                    fig.text(0.92, (i+0.75)/2.5, letter, va='center', fontsize=14, weight='bold')
                
            elif (X,Y) == (6,8):                         

                # 在底部加入 ABCDEF
                for i, letter in enumerate(['A', 'B', 'C', 'D', 'E', 'F']):
                    fig.text((i+1.25)/7.5, 0.05, letter, ha='center', fontsize=14, weight='bold')

                # 在右側加入 ABCDEFGH
                for i, letter in enumerate(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']):
                    fig.text(0.92, (i+1.1)/9.1, letter, va='center', fontsize=14, weight='bold')    

            elif (X,Y) == (4,8):                         

                # 在底部加入 ABCD
                for i, letter in enumerate(['A', 'B', 'C', 'D']):
                    fig.text((i+1.2)/5.5, 0.05, letter, ha='center', fontsize=14, weight='bold')

                # 在右側加入 ABCDEFGH
                for i, letter in enumerate(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']):
                    fig.text(0.92, (i+1.1)/9.1, letter, va='center', fontsize=14, weight='bold')

            plt.savefig('l6k_temp.jpg')
            image = Image.open("l6k_temp.jpg")
            image_bytes = io.BytesIO()
            image.save(image_bytes, format="JPEG")
            image_bytes.seek(0)

            sheet_2d_object_id_lst.append(self.fs_jpg.put(image_bytes, filename="l6k_temp.jpg"))

        plt.close('all')
        fig, axs = plt.subplots(Y, X, figsize=figsize_w)
        plt.subplots_adjust(left=left, bottom=bottom, right=right, top=top, wspace=wspace, hspace=hspace)
        
        for i in range(len(df)):

            charge_2d_r = pickle.loads(self.fs.get(ObjectId(df["2d_r_object_id"][i])).read())
            charge_2d_g = pickle.loads(self.fs.get(ObjectId(df["2d_g_object_id"][i])).read())
            charge_2d_b = pickle.loads(self.fs.get(ObjectId(df["2d_b_object_id"][i])).read())

            charge_2d = np.concatenate((charge_2d_r.flatten(), 
                                        charge_2d_g.flatten(), 
                                        charge_2d_b.flatten()))

            charge_2d_ori = np.zeros(W*H)

            charge_2d_ori[0::3] = charge_2d[:W*H//3]
            charge_2d_ori[1::3] = charge_2d[W*H//3:2*W*H//3]
            charge_2d_ori[2::3] = charge_2d[2*W*H//3:]  

            charge_2d_ori = np.reshape(charge_2d_ori,(H,W))  

            if (sheet_id[:2] in ["EM","EL"]):
                charge_2d_ori = np.rot90(charge_2d_ori, k=-1)            

            x = Y - 1 - (ord(df['chip_pos'][i][-1]) - ord('A'))
            y = ord(df['chip_pos'][i][-2]) - ord('A')                       
            
            plt.axis('on')
            axs[x, y].imshow(charge_2d_ori, cmap="Greys")
            axs[x, y].set_aspect('equal')

        # 清理所有子圖的 x 和 y ticks
        for ax in axs.flat:
            ax.set_xticks([])
            ax.set_yticks([])
            
        if (X,Y) == (2,2):

            # 在底部加入 AB
            for i, letter in enumerate(['A', 'B']):
                fig.text((i+0.75)/2.5, 0, letter, ha='center', fontsize=14, weight='bold')

            # 在右側加入 AB
            for i, letter in enumerate(['A', 'B']):
                fig.text(0.92, (i+0.75)/2.5, letter, va='center', fontsize=14, weight='bold')
            
        elif (X,Y) == (6,8):                         

            # 在底部加入 ABCDEF
            for i, letter in enumerate(['A', 'B', 'C', 'D', 'E', 'F']):
                fig.text((i+1.25)/7.5, 0, letter, ha='center', fontsize=14, weight='bold')

            # 在右側加入 ABCDEFGH
            for i, letter in enumerate(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']):
                fig.text(0.92, (i+1.1)/9.1, letter, va='center', fontsize=14, weight='bold')    

        elif (X,Y) == (4,8):                         

            # 在底部加入 ABCD
            for i, letter in enumerate(['A', 'B', 'C', 'D']):
                fig.text((i+1.2)/5.5, 0, letter, ha='center', fontsize=14, weight='bold')

            # 在右側加入 ABCDEFGH
            for i, letter in enumerate(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']):
                fig.text(0.92, (i+1.1)/9.1, letter, va='center', fontsize=14, weight='bold')                     

        plt.savefig('l6k_temp.jpg')
        image = Image.open("l6k_temp.jpg")
        image_bytes = io.BytesIO()
        image.save(image_bytes, format="JPEG")
        image_bytes.seek(0)

        sheet_2d_object_id_lst.append(self.fs_jpg.put(image_bytes, filename="l6k_temp.jpg"))

        table_schema = {'lot_start_time': df["lot_start_time"].unique()[0],
                        'sheet_start_time': sheet_time,
                        'eqp_id': df["eqp_id"].unique()[0],
                        'op_id': op_id,
                        'recipe_id': df["recipe_id"].unique()[0],
                        'lot_id': df["lot_id"].unique()[0],
                        'sheet_id': sheet_id,
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

    logging.basicConfig(filename=f'log/l6k_jpg_{datetime.today().strftime("%Y_%m_%d_%H_%M")}.log', filemode='w+', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    # 選擇 client
    client = MongoClient('mongodb://ivan:ivan@10.88.26.183:27017')
    # 選擇 Database
    db = client["AT_jpg"]
    # 選擇 collection
    collection_jpg = db["L6K_JPG"]
    # 創建 index 避免資料重複塞進資料庫
    collection_jpg.create_index([("sheet_start_time", 1),
                                ("lot_start_time", 1),
                                ("eqp_id", 1),
                                ("op_id", 1),
                                ("recipe_id", 1),
                                ("lot_id", 1),
                                ("sheet_id", 1),
                                ("step", 1),
                                ("charge_type", 1)], 
                            unique=True)    
    # 選擇 gridfs
    fs_jpg = gridfs.GridFS(db, collection="L6K_JPG")     
        
    logging.info("instantiate object")
    print("instantiate object")
    etl_obj = ETL(collection_jpg, fs_jpg)
    
    print("The current date and time is", datetime.now().strftime("%d/%m/%Y, %H:%M:%S"))

    logging.info("ETL L6K JPG")
    print("ETL L6K JPG")  
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