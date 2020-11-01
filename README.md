This project uses *AWS Cloud Development Kit*. Please follow the installation guide to install [AWS CDK Toolkit](https://cdkworkshop.com/15-prerequisites.html)

Install dependencies for the project by calling  
```
pip install -r requirements.txt -t .
```
from findAflat folder.

Then install the app's standard dependencies
```
pip install -r requirements.txt
```
and deploy CloudFormation stack
```
cdk deploy --app "python app.py"
```
