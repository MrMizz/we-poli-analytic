curl -X POST \
  -H 'Content-Type: application/json' \
  https://jskg4ocsd8.execute-api.us-west-2.amazonaws.com/prod/poli/graph -d '
{
  "TableName" : "PoliVertex",
  "Key" : {
    "uid": { "N": "4032920051055622454" }
  }
}'