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
        self.collection, self.fs = connect_MongoDB(client='mongodb://ivan:ivan@10.88.26.183:27017', db_name="AT", collection="L4A_charge2d")
        self.collection_jpg = collection_jpg
        self.fs_jpg = fs_jpg
        
    def etl(self):
        
        # sheet_lst = self.collection.find({'lm_time': {'$gte': (datetime.now()-timedelta(days=100)).strftime("%Y/%m/%d %H:%M:%S")}}).distinct("sheet_id")
        sheet_lst = self.collection.find({}).distinct("sheet_id")
        df = pd.DataFrame.from_records(self.collection.find({"sheet_id": {'$in': sheet_lst}}))  
        
        if df.empty:
            
            logging.info("時間內無資料")
            print("時間內無資料")
            
        else:
            
            df = df.drop_duplicates(['lm_time','eqp_id','op_id','recipe_id','lot_id','sheet_id','chip_id','ins_cnt','step','charge_type'])       
            df_all_sheet = df.drop_duplicates(['eqp_id','op_id','recipe_id','lot_id','sheet_id','ins_cnt','step','charge_type'])
            df_all_sheet = df_all_sheet.sort_values(by='lm_time', ascending=False)
            
            for _,df_sheet in df_all_sheet.iterrows():
                
                df_chip = df[(df['eqp_id']==df_sheet["eqp_id"]) &
                            (df['op_id']==df_sheet["op_id"]) &
                            (df['recipe_id']==df_sheet["recipe_id"]) &
                            (df['lot_id']==df_sheet["lot_id"]) &
                            (df['sheet_id']==df_sheet["sheet_id"]) &
                            (df['ins_cnt']==df_sheet["ins_cnt"]) &
                            (df['step']==df_sheet["step"]) &
                            (df['charge_type']==df_sheet["charge_type"])]   
                
                logging.info(df_sheet['lm_time'] + "," +\
                            df_sheet['eqp_id'] + "," +\
                            df_sheet['op_id'] + "," +\
                            df_sheet['recipe_id'] + "," +\
                            df_sheet['lot_id'] + "," +\
                            df_sheet['sheet_id'] + "," +\
                            df_sheet['ins_cnt'] + "," +\
                            df_sheet['step'] + "," +\
                            df_sheet['charge_type'] + "," + " : 共 " + str(len(df_chip)) + " 片")

                df_chip = df_chip.sort_values('chip_pos')
                df_chip = df_chip.reset_index(drop=True)  

                X = df_config[df_config["model"]==df_sheet["sheet_id"][:2]]["X"].values[0]
                Y = df_config[df_config["model"]==df_sheet["sheet_id"][:2]]["Y"].values[0]
                W = df_config[df_config["model"]==df_sheet["sheet_id"][:2]]["W"].values[0]
                H = df_config[df_config["model"]==df_sheet["sheet_id"][:2]]["H"].values[0]                                      
                                        
                # EK/EJ
                if (df_sheet["sheet_id"][:2] in ["EJ","EK"]):
                    
                    # 大板長寬
                    left, right, bottom, top, wspace, hspace = 0.1, 0.88, 0.12, 0.95, 0, 0
                    figsize_rgb = (4, 4.3)
                    figsize_w = (4, 1.4)
                
                # EE/EG
                elif (df_sheet["sheet_id"][:2] in ["EE","EG"]):
                    
                    # 大板長寬
                    left, right, bottom, top, wspace, hspace = 0.1, 0.91, 0.07, 0.95, 0, 0
                    figsize_rgb = (14, 10)
                    figsize_w = (12, 3)

                # EM/EL
                elif (df_sheet["sheet_id"][:2] in ["EM","EL"]):
                    
                    # 大板長寬
                    left, right, bottom, top, wspace, hspace = 0.1, 0.9, 0.08, 0.95, 0, 0
                    figsize_rgb = (6, 5)  
                    figsize_w = (4, 10)                           

                # GA/GB
                elif (df_sheet["sheet_id"][:2] in ["GA","GB"]):
                    
                    # 大板長寬
                    left, right, bottom, top, wspace, hspace = 0.1, 0.9, 0.08, 0.95, 0, 0
                    figsize_rgb = (10, 6.5)  
                    figsize_w = (24, 5.5)    
                    df_chip['chip_pos'] = df_chip['chip_pos'].str.replace('A', '10')
                    df_chip['chip_pos'] = df_chip['chip_pos'].str.replace('B', '11')                    
                    
                else:
                    continue

                self.plot_sheet(df_chip, X, Y, H, W,
                                df_sheet["lm_time"],
                                df_sheet["eqp_id"],
                                df_sheet["op_id"],
                                df_sheet["recipe_id"],
                                df_sheet["lot_id"],
                                df_sheet["sheet_id"],
                                df_sheet["ins_cnt"], 
                                df_sheet["step"],
                                df_sheet["charge_type"],
                                left, right, bottom, top, wspace, hspace,
                                figsize_rgb, figsize_w,
                                len(df_chip)
                                )
                

    def plot_sheet(self, df, X, Y, H, W,
                    lm_time, 
                    eqp_id,
                    op_id, 
                    recipe_id,
                    lot_id, 
                    sheet_id,  
                    ins_cnt,
                    step, 
                    charge_type,
                    left, right, bottom, top, wspace, hspace,
                    figsize_rgb, figsize_w,
                    chip_cnt):
        
        # config
        sheet_2d_object_id_lst = []
        color_dict = {"Reds":"2d_r_object_id",
                    "Greens":"2d_g_object_id",
                    "Blues":"2d_b_object_id"}

        # EK/EJ
        if (sheet_id[:2] in ["EJ","EK"]):
            matrix = [(i, j) for i in range(2) for j in range(2)]
        # EE/EG
        elif (sheet_id[:2] in ["EE","EG"]):
            matrix = [(i, j) for i in range(8) for j in range(6)]
        # EM/EL
        elif (sheet_id[:2] in ["EM","EL"]):
            matrix = [(i, j) for i in range(8) for j in range(4)]
        # EM/EL
        elif (sheet_id[:2] in ["GA","GB"]):
            matrix = [(i, j) for i in range(12) for j in range(2)]    
                    
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
                
                x = Y-1-int(df['chip_pos'][i].split(".")[0])
                y = int(df['chip_pos'][i].split(".")[-1])
                
                plt.axis('on')
                axs[x, y].imshow(arr, cmap=color)
                axs[x, y].set_aspect('equal')

                try:
                    matrix.remove((x,y))
                except:
                    pass 

            for xy in matrix:
                axs[xy[0], xy[1]].text(0.5, 0.5, 'X', fontsize=20, color='red', ha='center', va='center')
            
            # 清理所有子圖的 x 和 y ticks
            for ax in axs.flat:
                ax.set_xticks([])
                ax.set_yticks([])

            if (X,Y) == (2,2):

                # 在底部加入 01
                for i, letter in enumerate(['0', '1']):
                    fig.text((i+0.75)/2.5, 0.05, letter, ha='center', fontsize=14, weight='bold')

                # 在右側加入 01
                for i, letter in enumerate(['0', '1']):
                    fig.text(0.92, (i+0.75)/2.5, letter, va='center', fontsize=14, weight='bold')
                
            elif (X,Y) == (6,8):                         

                # 在底部加入 012345
                for i, letter in enumerate(['0', '1', '2', '3', '4', '5']):
                    fig.text((i+1.25)/7.5, 0.05, letter, ha='center', fontsize=14, weight='bold')

                # 在右側加入 01234567
                for i, letter in enumerate(['0', '1', '2', '3', '4', '5', '6', '7']):
                    fig.text(0.92, (i+1.1)/9.1, letter, va='center', fontsize=14, weight='bold') 

            elif (X,Y) == (4,8):                         

                # 在底部加入 0123
                for i, letter in enumerate(['0', '1', '2', '3']):
                    fig.text((i+1.2)/5.5, 0.05, letter, ha='center', fontsize=14, weight='bold')

                # 在右側加入 01234567
                for i, letter in enumerate(['0', '1', '2', '3', '4', '5', '6', '7']):
                    fig.text(0.92, (i+1.1)/9.1, letter, va='center', fontsize=14, weight='bold')                       

            elif (X,Y) == (2,12):

                # 在底部加入 01
                for i, letter in enumerate(['0', '1']):
                    fig.text((i+0.75)/2.5, 0.05, letter, ha='center', fontsize=14, weight='bold')

                # 在右側加入 0123456789AB
                for i, letter in enumerate(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B']):
                    fig.text(0.92, (i+1.65)/14, letter, va='center', fontsize=14, weight='bold')  
        
            plt.savefig('l4a_temp.jpg')
            image = Image.open("l4a_temp.jpg")
            image_bytes = io.BytesIO()
            image.save(image_bytes, format="JPEG")
            image_bytes.seek(0)

            sheet_2d_object_id_lst.append(self.fs_jpg.put(image_bytes, filename="l4a_temp.jpg"))

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

            x = Y-1-int(df['chip_pos'][i].split(".")[0])
            y = int(df['chip_pos'][i].split(".")[-1])
                            
            plt.axis('on')
            axs[x, y].imshow(charge_2d_ori, cmap="Greys")
            axs[x, y].set_aspect('equal')
            
        for xy in matrix:
            axs[xy[0], xy[1]].text(0.5, 0.5, 'X', fontsize=20, color='red', ha='center', va='center')
                        
        # 清理所有子圖的 x 和 y ticks
        for ax in axs.flat:
            ax.set_xticks([])
            ax.set_yticks([])
            
        if (X,Y) == (2,2):

            # 在底部加入 01
            for i, letter in enumerate(['0', '1']):
                fig.text((i+0.75)/2.5, 0, letter, ha='center', fontsize=14, weight='bold')

            # 在右側加入 01
            for i, letter in enumerate(['0', '1']):
                fig.text(0.92, (i+0.75)/2.5, letter, va='center', fontsize=14, weight='bold')
            
        elif (X,Y) == (6,8):                         

            # 在底部加入 012345
            for i, letter in enumerate(['0', '1', '2', '3', '4', '5']):
                fig.text((i+1.25)/7.5, 0, letter, ha='center', fontsize=14, weight='bold')

            # 在右側加入 01234567
            for i, letter in enumerate(['0', '1', '2', '3', '4', '5', '6', '7']):
                fig.text(0.92, (i+1.1)/9.1, letter, va='center', fontsize=14, weight='bold') 

        elif (X,Y) == (4,8):                         

            # 在底部加入 0123
            for i, letter in enumerate(['0', '1', '2', '3']):
                fig.text((i+1.2)/5.5, 0, letter, ha='center', fontsize=14, weight='bold')

            # 在右側加入 01234567
            for i, letter in enumerate(['0', '1', '2', '3', '4', '5', '6', '7']):
                fig.text(0.92, (i+1.1)/9.1, letter, va='center', fontsize=14, weight='bold')                       

        elif (X,Y) == (2,12):

            # 在底部加入 0123
            for i, letter in enumerate(['0', '1']):
                fig.text((i+0.75)/2.5, 0.05, letter, ha='center', fontsize=14, weight='bold')

            # 在右側加入 01234567
            for i, letter in enumerate(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B']):
                fig.text(0.92, (i+1.65)/14, letter, va='center', fontsize=14, weight='bold')  
            
        plt.savefig('l4a_temp.jpg')
        image = Image.open("l4a_temp.jpg")
        image_bytes = io.BytesIO()
        image.save(image_bytes, format="JPEG")
        image_bytes.seek(0)
        
        sheet_2d_object_id_lst.append(self.fs_jpg.put(image_bytes, filename="l4a_temp.jpg"))
        
        table_schema = {'lm_time': lm_time,
                        'eqp_id': eqp_id,
                        'op_seq': op_id,
                        'recipe_id': recipe_id,
                        'lot_id': lot_id,
                        'sheet_id': sheet_id,
                        'ins_cnt': ins_cnt,
                        'step': step,
                        'charge_type': charge_type,
                        'chip_cnt': chip_cnt
                        }
        
        if not pd.DataFrame.from_records(self.collection_jpg.find(table_schema)).empty:
            
            for col in ["2d_b_object_id", "2d_g_object_id", "2d_r_object_id", "2d_w_object_id"]:

                object_id = pd.DataFrame.from_records(self.collection_jpg.find(table_schema))[col][0]
                self.fs_jpg.delete(object_id)
            
        self.collection_jpg.update_one(
            table_schema, 
            {"$set": {"2d_r_object_id": sheet_2d_object_id_lst[0],
                        "2d_g_object_id": sheet_2d_object_id_lst[1],
                        "2d_b_object_id": sheet_2d_object_id_lst[2],
                        "2d_w_object_id": sheet_2d_object_id_lst[3]
                        }},
            upsert=True)

        del table_schema  

def job():

    logging.basicConfig(filename=f'log/l4a_jpg_{datetime.today().strftime("%Y_%m_%d_%H_%M")}.log', filemode='w+', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

    # 選擇 client
    client = MongoClient('mongodb://ivan:ivan@10.88.26.183:27017')
    # 選擇 Database
    db = client["AT_jpg"]
    # 選擇 collection
    collection_jpg = db["L4A_JPG"]  
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

    while True:  
        schedule.run_pending()    
        time.sleep(1)
