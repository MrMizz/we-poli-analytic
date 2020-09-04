curl -X POST \
  -H 'Content-Type: application/json' \
  https://yf87qmn85l.execute-api.us-west-2.amazonaws.com/v1/poli/vertex -d '
{
  "RequestItems": {
    "PoliEdge": {
      "Keys": [
        {
          "src_id": {
            "N": "100386987"
          },
          "dst_id": {
            "N": "1072020170035548574"
          }
        }
      ]
    }
  }
}'