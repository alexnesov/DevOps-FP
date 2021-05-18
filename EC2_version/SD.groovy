 
pipeline {
    agent any
    stages {
        stage('EOODATA') {
            steps {
                sh '''
                    #!/bin/bash
                    . /var/lib/jenkins/virtualenvs/test-env/bin/activate
                    cd /home/ubuntu/eoddata
                    . /home/ubuntu/.bashrc; python -u /home/ubuntu/eoddata/eoddata.py > /home/ubuntu/eoddata/downloads/logs/eod.log
                '''
        }
    }
        stage('eod transfer') {
            steps {
                sh '''
                    #!/bin/bash
                    . /var/lib/jenkins/virtualenvs/test-env/bin/activate
                    cd /home/ubuntu/eoddata
                    . /home/ubuntu/.bashrc; python -u /home/ubuntu/eoddata/eoddata.py >> /home/ubuntu/eoddata/downloads/logs/eod.log
                '''
        }
    }
        stage('Signal_detection') {
            steps {
                sh '''
                    #!/bin/bash
                    . /var/lib/jenkins/virtualenvs/test-env/bin/activate
                    cd /home/ubuntu/Signal-Detection
                    . /home/ubuntu/.bashrc; python -u /home/ubuntu/Signal-Detection/Signal_detection.py > /home/ubuntu/Signal-Detection/utils/Signal_detection.log 2>&1
                '''
        }
    }
        stage('Signal_detection_evol') {
            steps {
                sh '''
                    #!/bin/bash
                    . /var/lib/jenkins/virtualenvs/test-env/bin/activate
                    cd /home/ubuntu/Signal-Detection
                    . /home/ubuntu/.bashrc; python -u /home/ubuntu/Signal-Detection/signalsEvol.py > /home/ubuntu/Signal-Detection/utils/signalsEvol.log 2>&1
                '''
            }
        }
        stage('Detailed_generation') {
            steps {
                sh '''
                    #!/bin/bash
                    . /var/lib/jenkins/virtualenvs/test-env/bin/activate
                    cd /home/ubuntu/Signal-Detection
                    . /home/ubuntu/.bashrc; python -u /home/ubuntu/Signal-Detection/DetailedGeneration.py > /home/ubuntu/Signal-Detection/utils/DetailedGeneration.log 2>&1
                '''
        }
    }
}
}