sed 's/,$//' iris.data | cut -d',' -f1-4 | tr ',' ' ' > dataset.txt
