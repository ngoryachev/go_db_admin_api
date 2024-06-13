# Dockerfile

# Используем официальный образ Golang
FROM golang:1.20-alpine

# Устанавливаем git
RUN apk add --no-cache git

# Устанавливаем рабочую директорию
WORKDIR /app

# Клонируем репозиторий
RUN git clone -b feature/naiv https://github.com/ngoryachev/go_db_admin_api.git .

# Выводим ветку
RUN cd /app && git rev-parse --abbrev-ref HEAD

# Компилируем программу
RUN go build -o go_db_admin_api

# Устанавливаем порт, который будет слушать приложение
EXPOSE 8082

# Запускаем приложение
CMD ["./go_db_admin_api"]