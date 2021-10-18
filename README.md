# file-storage

Сервис, обращающийся напрямую к s3 через AWS JAVA SDK. Используется для генерации pre-signed URL that can be used to
access an Amazon S3 resource without requiring the user of the URL to know the account's AWS security credentials.

## Параметры запуска

Для работы с 1 версией `AWS SDK S3`

```yaml
s3:
  endpoint: 'http://127.0.0.1:32827'
  bucket-name: 'files'
  signing-region: 'RU'
  client-protocol: 'http'
  client-max-error-retry: 10
  signer-override: 'S3SignerType'
  #  signer-override: 'AWSS3V4SignerType'
  access-key: 'test'
  secret-key: 'test'
s3-sdk-v2:
  enabled: 'false'
```

дефолтная версия сигнера — `S3SignerType`, для использования более актуальной версии указывается `AWSS3V4SignerType`

Для работы с 2 версией `AWS SDK S3 V2`

```yaml
s3-sdk-v2:
  enabled: 'true'
  endpoint: 'http://127.0.0.1:9000'
  bucket-name: 'files-v2'
  region: 'RU'
  access-key: 'minio'
  secret-key: 'minio123'
```

Для работы сервиса может использоваться только одна из двух версий `AWS SDK S3`, переключение происходит
параметром `s3-sdk-v2.enabled=false`

## Minio

Если сервисом используется 2 версия `AWS SDK S3 V2`, и в качестве s3 кластера используется `minio`, то для поддержки
версионирования объектов __кластер должен использовать минимум несколько драйверов при старте__ для включения
механизма `Erasure Code`

Для включения механизма `Erasure Code` запуск сервера `minio` с использованием нескольких драйверов может выглядеть
следующим образом

```shell
minio server /data{1...12}
```

Цитата из официальной документации
> **Versioning feature is only available in erasure coded and distributed erasure coded setups.**

Источники

- [versioning-guide](https://docs.min.io/docs/minio-bucket-versioning-guide.html)
- [erasure-code-quickstart-guide](https://docs.min.io/docs/minio-erasure-code-quickstart-guide)

В репозитории в папке [minio-local-cluster](./minio-local-cluster/) содержатся примеры `docker-compose` манифестов
(спизж**ных из официальной репы https://github.com/minio/minio/tree/master/docs/orchestration/docker-compose)
для локального запуска сервера `minio` с включенным механизмом `Erasure Code`
