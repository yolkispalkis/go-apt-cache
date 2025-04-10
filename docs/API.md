# API Reference go-apt-proxy

## Общие сведения

go-apt-proxy предоставляет HTTP API для доступа к пакетам и метаданным APT-репозиториев. API состоит из нескольких эндпоинтов, которые описаны ниже.

## Эндпоинты

### GET /{repository}/{path}

Основной эндпоинт для проксирования запросов к репозиториям. Позволяет получить файлы из репозитория.

#### Параметры

- `repository` - имя репозитория, настроенного в конфигурации
- `path` - путь к файлу относительно корня репозитория

#### Пример запроса

```
GET /ubuntu/dists/focal/Release HTTP/1.1
Host: localhost:8080
```

#### Пример ответа

```
HTTP/1.1 200 OK
Content-Type: text/plain
Last-Modified: Wed, 23 Apr 2023 12:34:56 GMT
Content-Length: 12345

Origin: Ubuntu
Label: Ubuntu
Suite: focal
Version: 20.04
...
```

#### Коды ответа

- `200 OK` - файл найден и отправлен
- `304 Not Modified` - файл не изменился с момента последнего запроса
- `404 Not Found` - файл не найден
- `500 Internal Server Error` - внутренняя ошибка сервера
- `502 Bad Gateway` - ошибка при обращении к upstream-репозиторию
- `504 Gateway Timeout` - таймаут при обращении к upstream-репозиторию

#### Поддерживаемые заголовки запроса

- `If-Modified-Since` - возвращает 304, если файл не изменился с указанной даты
- `Range` - запрос диапазона байтов для частичной загрузки файла

### GET /status

Эндпоинт для проверки состояния сервера и кеша.

#### Пример запроса

```
GET /status HTTP/1.1
Host: localhost:8080
```

#### Пример ответа

```
HTTP/1.1 200 OK
Content-Type: text/plain

OK
Cache Items: 123
Cache Size: 2.5GB / 10GB
```

#### Коды ответа

- `200 OK` - сервер работает нормально
- `500 Internal Server Error` - сервер работает, но есть проблемы

## Заголовки HTTP

### Запросы

go-apt-proxy корректно обрабатывает следующие заголовки запросов:

- `If-Modified-Since` - условное получение, если ресурс изменился
- `Range` - запрос диапазона байтов для частичной загрузки
- `If-Range` - условное получение диапазона
- `Accept-Encoding` - поддерживаемые клиентом кодировки содержимого

### Ответы

go-apt-proxy устанавливает следующие заголовки в ответах:

- `Content-Type` - тип содержимого (определяется по расширению файла)
- `Content-Length` - размер содержимого в байтах
- `Last-Modified` - дата последнего изменения файла
- `ETag` - идентификатор версии ресурса (только если предоставлен upstream)
- `Accept-Ranges` - поддерживаемые типы диапазонов (обычно "bytes")
- `Cache-Control` - директивы кеширования (проксируются из upstream)
- `Expires` - дата истечения срока действия (проксируется из upstream)

## Примеры использования API

### Получение списка релизов

```bash
curl http://localhost:8080/ubuntu/dists/
```

### Получение файла Release

```bash
curl http://localhost:8080/ubuntu/dists/focal/Release
```

### Проверка статуса сервера

```bash
curl http://localhost:8080/status
```

### Условное получение с If-Modified-Since

```bash
curl -H "If-Modified-Since: Wed, 23 Apr 2023 12:34:56 GMT" http://localhost:8080/ubuntu/dists/focal/Release
```

### Частичная загрузка с Range

```bash
curl -H "Range: bytes=0-1023" http://localhost:8080/ubuntu/pool/main/l/linux/linux-image-5.4.0-26-generic_5.4.0-26.30_amd64.deb
```

## Ограничения

- API не поддерживает запись данных в репозитории (только чтение)
- Не поддерживается аутентификация (предполагается использование в защищенной сети)
- Не поддерживается листинг директорий (возвращается ошибка 403 Forbidden)
- Максимальный размер запроса ограничен конфигурацией Go HTTP-сервера

## Обработка ошибок

В случае ошибки сервер возвращает соответствующий код HTTP-ответа и текстовое сообщение об ошибке в теле ответа.

Пример ответа с ошибкой:

```
HTTP/1.1 404 Not Found
Content-Type: text/plain
Content-Length: 13

404 Not Found
```

## Кеширование на стороне клиента

go-apt-proxy поддерживает стандартный механизм кеширования HTTP:

1. Клиент может указать заголовок `If-Modified-Since` для проверки актуальности кешированного ресурса.
2. Если ресурс не изменился, сервер вернет статус `304 Not Modified` без тела ответа.
3. Клиент может использовать локально кешированную версию ресурса.

Это позволяет APT клиентам эффективно проверять обновления, не загружая повторно неизмененные файлы. 