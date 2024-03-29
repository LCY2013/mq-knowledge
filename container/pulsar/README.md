# 启动后操作

```shell
CSRF_TOKEN=$(curl http://{IP}:7750/pulsar-manager/csrf-token)

curl \
    -H "X-XSRF-TOKEN: $CSRF_TOKEN" \
    -H "Cookie: XSRF-TOKEN=$CSRF_TOKEN;" \
    -H 'Content-Type: application/json' \
    -X PUT http://{IP}:7750/pulsar-manager/users/superuser \
    -d '{"name": "admin", "password": "apachepulsar", "description": "test", "email": "username@test.org"}'

./pulsar-admin clusters create cluster-1 \
    --url http://{IP}:8080 \
    --broker-url pulsar://{IP}:6650

./pulsar-admin clusters update cluster-a \
    --url http://{IP}:8080 \
    --broker-url pulsar://{IP}:6650
```