curl -X POST \
  -H 'Content-Type: application/json' \
  https://yf87qmn85l.execute-api.us-west-2.amazonaws.com/v1/poli/vertex -d '
{
  "RequestItems": {
    "PoliTraversalsPage": {
      "Keys": [
        {
          "vertex_id": {
            "N": "100386987"
          },
          "page_num": {
            "N": "1"
          }
        }
      ]
    }
  }
}'