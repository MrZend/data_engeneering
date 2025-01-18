FROM python:3.12-slim 

RUN pip install jupyterlab \
    && mkdir /opt/notebooks

EXPOSE 8888 

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--LabApp.token=''"] 

