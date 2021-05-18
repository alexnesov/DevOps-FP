pipeline {
    agent any
    stages {
        stage('FINVIZ') {
            steps {
                sh '''
                    #!/bin/bash
                    . /var/lib/jenkins/virtualenvs/test-env/bin/activate
                    cd /home/ubuntu/financials-downloader-bot
                    . /home/ubuntu/.bashrc; python -u /home/ubuntu/financials-downloader-bot/run.py > /home/ubuntu/financials-downloader-bot/crontab.log 2>&1
                '''
        }
    }
        stage('Transfer_technicals') {
            steps {
                sh '''
                    #!/bin/bash
                    . /var/lib/jenkins/virtualenvs/test-env/bin/activate
                    cd /home/ubuntu/Signal-Detection
                    . /home/ubuntu/.bashrc; python -u /home/ubuntu/Signal-Detection/transferTechnicals.py
                '''
            }
        }
        stage('Transfer_ownership') {
            steps {
                sh '''
                    #!/bin/bash
                    . /var/lib/jenkins/virtualenvs/test-env/bin/activate
                    cd /home/ubuntu/Signal-Detection
                    . /home/ubuntu/.bashrc; python -u /home/ubuntu/Signal-Detection/transferOwnership.py
                '''
        }
    }
        stage('SP500') {
            steps {
                sh '''
                    #!/bin/bash
                    . /var/lib/jenkins/virtualenvs/test-env/bin/activate
                    cd /home/ubuntu/Signal-Detection
                    . /home/ubuntu/.bashrc; python -u /home/ubuntu/Signal-Detection/sp500.py
                '''
        }
    }
}
}