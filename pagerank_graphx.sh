source /home/ubuntu/run.sh
python /home/ubuntu/assignment2/question_a/cleanup_input.py /home/ubuntu/input_data/soc-LiveJournal1.txt
hadoop fs -copyFromLocal /home/ubuntu/input_data/soc-LiveJournal1.txt /user/ubuntu/
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
ssh -t ubuntu@vm-4-2 "sudo sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""
ssh -t ubuntu@vm-4-3 "sudo sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""
ssh -t ubuntu@vm-4-4 "sudo sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""
ssh -t ubuntu@vm-4-5 "sudo sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""
for z in 1 2 3 4 5
do
	ssh vm-4-$z mkdir -p /home/ubuntu/output_stats/graphx_pagerank/
	ssh -t vm-4-$z "sudo cp /proc/net/dev /home/ubuntu/output_stats/graphx_pagerank/net_before"
	ssh -t vm-4-$z "sudo cp /proc/diskstats /home/ubuntu/output_stats/graphx_pagerank/disk_before"
done
START=$(date +%s)
hadoop fs -rmr /user/ubuntu/pagerankgraphx_output
spark-submit --verbose /home/ubuntu/assignment3/Part-B/PartBApplication1Question1/target/scala-2.10/partbapplication1question1_2.10-1.0.jar soc-LiveJournal1.txt
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Program run time : $DIFF">>time_graphx_pagerank
for z in 1 2 3 4 5
do
	ssh -t vm-4-$z "sudo cp /proc/net/dev /home/ubuntu/output_stats/graphx_pagerank/net_after"
        ssh -t vm-4-$z "sudo cp /proc/diskstats /home/ubuntu/output_stats/graphx_pagerank/disk_after"
done

