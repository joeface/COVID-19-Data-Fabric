FROM python:3.8


# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

ADD main.py /
ADD requirements.txt /

# Set up and activate virtual environment
ENV VIRTUAL_ENV "/venv"
RUN python -m venv $VIRTUAL_ENV
ENV PATH "$VIRTUAL_ENV/bin:$PATH"
ENV MANUAL_DATA_SOURCE_URL "https://docs.google.com/spreadsheets/d/e/2PACX-1vTRXudbSwPQY2DJDcYQ3Rot7TxLR1I8HzepeRuhU6VRAcVCnKKDS7wNvku0VlX0yg_fv7eiXpd41YWK/pub?gid=0&single=true&output=csv"


RUN python -m pip install -r requirements.txt
CMD [ "python", "./main.py" ]