curl -X POST \
  -H 'Content-Type: application/json' \
  https://yf87qmn85l.execute-api.us-west-2.amazonaws.com/v1/poli/vertex -d '
{
  "RequestItems": {
    "PoliVertex": {
      "Keys": [
        {
          "uid": {
            "N": "4032920051055622454"
          }
        },
        {
          "uid": {
            "N": "4051820041038596757"
          }
        }
      ]
    }
  }
}'