docker exec -it spark-master ./bin/spark-submit \
	--master spark://spark-master:7077 \
	./work/simple-app.py