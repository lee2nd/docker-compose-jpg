#!/bin/bash

# 定義需要停止和刪除的容器名稱
containers=("l4a_jpg" "l6b_fs_eb16c_jpg" "l6b_fs_eb17c_jpg" "l6b_sw_at2_jpg" "l6b_sw_tc01_jpg" "l6b_sw_tc02_jpg")

# 停止並刪除現有容器，如果存在
for container in "${containers[@]}"; do
    if [ "$(docker ps -aq -f name=$container)" ]; then
        echo "Stopping existing container: $container..."
        docker stop $container
        echo "Removing existing container: $container..."
        docker rm $container
    fi
done

# 使用 Docker Compose 啟動新的容器
USER_ID=$(id -u) GROUP_ID=$(id -g) docker-compose up -d
