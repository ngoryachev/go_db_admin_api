# Dockerfile

# Используем официальный образ Golang
FROM golang:1.20-alpine

# Устанавливаем git
RUN apk add --no-cache git

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем локальные файлы в контейнер
COPY . .

# Компилируем программу
RUN go build -o go_db_admin_api

# Устанавливаем порт, который будет слушать приложение
EXPOSE 8082

# Запускаем приложение
CMD ["./go_db_admin_api"]