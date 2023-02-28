
sleep 1s;

for x in {0..30}; do
    wget http://127.0.0.1:15077/set_job_completed?jobId=job${x}

    sleep 0.1s;
done;
