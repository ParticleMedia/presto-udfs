# As Presto UDF might not be loaded successfully which will make Presto service unavailable. So this script
# should be triggered manually after careful verification in staging env.

# This script will try to see if presto service can start after deploy the new udf, is failed, will try to rollback
# to the original jar or remove the jar if no jar in the beginning.


jar_name="udfs-3.0.1-SNAPSHOT.jar"
download_file_path="/tmp/${jar_name}"
backup_file_path="/tmp/${jar_name}.bak"
udf_dir="/usr/lib/presto/plugin/presto-udf"
existing_jar="${udf_dir}/${jar_name}"
s3_path="s3://data-emr-pmi/presto-bootstrap/jar/${jar_name}"

date

echo "Step 0: Check to see if there's a running presto udf. If yes, copy to a backup location for rollback."
if [ -f "$existing_jar" ]
  then
  sudo cp "$existing_jar" "$backup_file_path"
  echo "Step 0: Copied ${existing_jar} into ${backup_file_path}"
fi

echo "Step 1: Check to see if presto udf folder exists..."
if [ ! -d "$udf_dir" ]
  then
  echo "Step 1.1: No udf folder found, will create ${udf_dir}"
  sudo mkdir "$udf_dir"
fi

echo "Step 2: Check to see if new jar is the same as old jar."
if [ -f "$existing_jar" ]
then
  origin_md5=`md5sum  $existing_jar`
  origin_arr=($origin_md5)
  origin_md5_value=${origin_arr[0]}

  download_file_path="$download_file_path"
  new_md5=`md5sum $download_file_path`
  new_arr=($new_md5)
  new_md5_value=${new_arr[0]}

  if [[ "$origin_md5_value" == "$new_md5_value" ]];then
    echo "Step 2:skip. mds same."
    exit 0
  else
    echo "Step 2: Found new version of udf, will start update process."
  fi
else
  echo "Step 2: No existing jar found, will start download process."
fi

echo "Step 3: Copy s3 presto udf jar into ${udf_dir}"
sudo aws s3 cp "$s3_path" /tmp
sudo cp "$download_file_path" "$existing_jar"


echo "Step 4: Try to restart presto service..."
sudo systemctl restart presto-server.service
echo "Step 4: Sleep for a while and check system status..."
sleep 60
system_status=$(systemctl is-active presto-server.service)
if [ "$system_status" = "active" ]
  then
  echo "Final: Presto service active. Done."
  exit 0
else
  echo "Step 5.1: Presto service not healthy. Returned result ${system_status}. Will rollback."
  if [ -f "$backup_file_path" ]
  then
    echo "Step 5.1.1: Found backup presto udf, will rollback."
    sudo cp $backup_file_path $existing_jar
  else
    echo "Step 5.1.2: No backup file found. Delete current udf and restart."
    sudo rm $existing_jar
  fi
  sudo systemctl restart presto-server.service
  sleep 60
  system_status=$(systemctl is-active presto-server.service)
  if [ "$system_status" = "active" ]
    then
    echo "Final: Presto service successfully rolled back. Done."
  else
    echo "Final: ERROR! Failed to rollback, Presto in unavailable state."
  fi
fi
