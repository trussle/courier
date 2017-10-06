FROM iron/go

EXPOSE 8080

WORKDIR /app
ADD courier /app/

ENTRYPOINT ["./courier"]
