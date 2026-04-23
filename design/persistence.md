Example:
Stores configs as key value pairs
key: "services/auth/instances/1"
value: {
  "ip": "10.0.0.1",
  "port": 8080
}
To improve versioning and support compare and swap
{
  "value": {
    "ip": "10.0.0.1",
    "port": 8080
  },
  "version": 3
}