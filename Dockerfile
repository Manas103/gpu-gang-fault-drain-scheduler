FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/simulate ./cmd/simulate

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/simulate /simulate
ENTRYPOINT ["/simulate"]
