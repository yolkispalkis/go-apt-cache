# Создание и установка deb-пакета

В этом документе описан процесс создания deb-пакета для go-apt-proxy и его использование в системах на базе Debian/Ubuntu.

## Предварительные требования

Для сборки deb-пакета вам потребуются следующие пакеты:

```bash
sudo apt-get update
sudo apt-get install build-essential fakeroot dpkg-dev
```

Также убедитесь, что у вас установлен Go версии 1.18 или новее.

## Сборка пакета

1. Клонируйте репозиторий:

```bash
git clone https://github.com/yolkispalkis/go-apt-cache.git
cd go-apt-cache
```

2. Запустите скрипт сборки:

```bash
chmod +x build-deb.sh
./build-deb.sh
```

После успешного выполнения в директории `build/` будет создан файл `go-apt-proxy_1.0.0_amd64.deb`.

## Установка пакета

Установите созданный пакет с помощью команды:

```bash
sudo dpkg -i build/go-apt-proxy_1.0.0_amd64.deb
```

Если возникли проблемы с зависимостями, выполните:

```bash
sudo apt-get install -f
```

## Настройка

После установки пакета, сервис `go-apt-proxy` будет автоматически запущен и добавлен в автозагрузку.

Основные файлы:
- Исполняемый файл: `/usr/local/bin/go-apt-proxy`
- Конфигурация: `/etc/go-apt-proxy/config.json`
- Файлы кеша: `/var/cache/go-apt-proxy/`
- Лог-файлы: `/var/log/go-apt-proxy/`
- Systemd сервис: `/etc/systemd/system/go-apt-proxy.service`
- Конфигурация APT: `/etc/apt/apt.conf.d/01proxy`

### Редактирование конфигурации

Отредактируйте файл конфигурации по необходимости:

```bash
sudo nano /etc/go-apt-proxy/config.json
```

После изменения конфигурации перезапустите сервис:

```bash
sudo systemctl restart go-apt-proxy
```

### Управление сервисом

- Запуск сервиса: `sudo systemctl start go-apt-proxy`
- Остановка сервиса: `sudo systemctl stop go-apt-proxy`
- Перезапуск сервиса: `sudo systemctl restart go-apt-proxy`
- Проверка статуса: `sudo systemctl status go-apt-proxy`
- Включение автозапуска: `sudo systemctl enable go-apt-proxy`
- Отключение автозапуска: `sudo systemctl disable go-apt-proxy`

## Удаление

Для удаления пакета:

```bash
sudo dpkg -r go-apt-proxy
```

Для полного удаления пакета с конфигурацией:

```bash
sudo dpkg --purge go-apt-proxy
```

## Проверка работоспособности

После установки проверьте, что сервис работает:

```bash
sudo systemctl status go-apt-proxy
```

Затем обновите индекс пакетов для проверки работы прокси:

```bash
sudo apt-get update
```

В логах сервиса должны появиться записи о запросах:

```bash
sudo tail -f /var/log/go-apt-proxy/apt_cache.log
```

## Поиск и устранение неисправностей

### Сервис не запускается

Проверьте журнал systemd:

```bash
sudo journalctl -u go-apt-proxy
```

### Медленная работа или ошибки кеширования

Проверьте настройки кеша в конфигурационном файле и наличие свободного места:

```bash
sudo df -h /var/cache/go-apt-proxy
```

При необходимости увеличьте размер кеша в файле `/etc/go-apt-proxy/config.json` и перезапустите сервис.

### Проблемы с правами доступа

Проверьте, что пользователь `apt-proxy` имеет права на запись в директории кеша и логов:

```bash
sudo ls -la /var/cache/go-apt-proxy /var/log/go-apt-proxy
```

При необходимости исправьте права доступа:

```bash
sudo chown -R apt-proxy:apt-proxy /var/cache/go-apt-proxy /var/log/go-apt-proxy
sudo chmod 750 /var/cache/go-apt-proxy /var/log/go-apt-proxy
``` 