pipeline {
    agent any
    stages 
        {
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