## Notes for SWB-#16:

### Additional libraries needed
`pip install dropbox`

You need usdm version of at least 0.5:

`pip install --upgrade usdm`


### For getting local upload working:
#### Add to environment file (.development_env)
`UPLOAD_SERVICE='local'`

`LOCAL_BASE="http://localhost:8014/v1/studyFiles/"`