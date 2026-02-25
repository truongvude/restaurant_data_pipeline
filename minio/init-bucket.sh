#!/bin/sh
set -e

echo "Waiting for MinIO start..."
while ! /usr/bin/mc alias set locals3 http://minio:9000 minio minio123; do 
    echo "MinIO not up and running yet..."
    sleep 1
done

/usr/bin/mc mb locals3/lakehouse;
echo "Created bucket: lakehouse."
/usr/bin/mc admin user add locals3 iamadmin iampassword;
echo "Added IAM user iamadmin."
/usr/bin/mc admin policy attach locals3 readwrite --user iamadmin;
echo "Attached policy for user iamadmin."
exit 0;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            