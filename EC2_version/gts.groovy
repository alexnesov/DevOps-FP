pipeline {
    agent any
    stages {
        stage('test') {
            steps {
                sh '''
                    #!/bin/bash
                    . /var/lib/jenkins/virtualenvs/test-env/bin/activate
                    cd /home/ubuntu/Signal-Detection
                    pip install -r requirements.txt
                    python test_jen.py
                '''
               
        }
    }
}
}
