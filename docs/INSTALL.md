# Руководство по установке и настройке go-apt-proxy

## Требования

- Go 1.18 или новее
- Доступ к файловой системе для хранения кешированных файлов
- Права на открытие сетевых портов (если используется TCP) или создание Unix сокетов

## Установка из исходного кода

1. Клонировать репозиторий:

```bash
git clone https://github.com/yolkispalkis/go-apt-cache.git
cd go-apt-cache
```

2. Скомпилировать исходный код:

```bash
go build -o go-apt-proxy main.go
```

3. Создать конфигурационный файл по умолчанию:

```bash
./go-apt-proxy -create-config -config config.json
```

4. Отредактировать конфигурационный файл `config.json` в соответствии с вашими потребностями.

## Настройка в качестве системного сервиса

### Systemd (Linux)

1. Создайте файл сервиса `/etc/systemd/system/go-apt-proxy.service`:

```ini
[Unit]
Description=Go APT Proxy Service
After=network.target

[Service]
ExecStart=/path/to/go-apt-proxy -config /path/to/config.json
Restart=on-failure
User=apt-proxy
Group=apt-proxy
WorkingDirectory=/path/to/working/directory

# Настройки безопасности (опционально)
ProtectSystem=full
ProtectHome=true
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

2. Создайте пользователя для запуска сервиса (если необходимо):

```bash
sudo useradd -r -s /bin/false apt-proxy
```

3. Создайте директории для кеша и логов, настройте права доступа:

```bash
sudo mkdir -p /var/cache/apt-proxy /var/log/apt-proxy
sudo chown apt-proxy:apt-proxy /var/cache/apt-proxy /var/log/apt-proxy
```

4. Включите и запустите сервис:

```bash
sudo systemctl daemon-reload
sudo systemctl enable go-apt-proxy
sudo systemctl start go-apt-proxy
```

5. Проверьте статус сервиса:

```bash
sudo systemctl status go-apt-proxy
```

## Настройка клиентов APT

### Для всех пользователей

Создайте файл `/etc/apt/apt.conf.d/01proxy`:

```
Acquire::http::Proxy "http://localhost:8080";
```

### Для отдельного пользователя

Создайте файл `~/.apt/apt.conf`:

```
Acquire::http::Proxy "http://localhost:8080";
```

### Временное использование

Для временного использования прокси в командной строке:

```bash
sudo apt-get update -o Acquire::http::Proxy="http://localhost:8080"
```

## Использование Unix сокета

1. Настройте параметр `unixSocketPath` в конфигурации:

```json
"server": {
  "listenAddress": "",
  "unixSocketPath": "/var/run/apt-proxy/apt-proxy.sock",
  "unixSocketPermissions": "660"
}
```

2. Удостоверьтесь, что пользователь имеет права на создание и запись в указанную директорию.

3. В конфигурации APT используйте:

```
Acquire::http::Proxy "http://unix:/var/run/apt-proxy/apt-proxy.sock";
```

## Проверка работоспособности

1. Запустите обновление кеша пакетов:

```bash
apt-get update
```

2. Проверьте лог-файлы на наличие ошибок:

```bash
tail -f /var/log/apt-proxy/apt_cache.log
```

3. Проверьте статус прокси:

```bash
curl http://localhost:8080/status
```

## Устранение неполадок

### Нет доступа к прокси

- Проверьте, запущен ли сервис
- Проверьте параметры сети (брандмауэр, NAT и т.д.)
- Проверьте права доступа к Unix сокету (если используется)

### Ошибки кеширования

- Проверьте права доступа к директории кеша
- Убедитесь, что достаточно места на диске
- Увеличьте значение `maxSize` в конфигурации, если необходимо

### Медленная работа

- Увеличьте значение `maxConcurrentFetches` для более агрессивного параллельного скачивания
- Проверьте сетевое подключение к upstream-репозиториям
- Убедитесь, что диск для кеша имеет достаточную производительность 