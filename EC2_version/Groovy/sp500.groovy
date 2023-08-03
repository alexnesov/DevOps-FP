pipeline {
    agent any
    stages {
        stage('SP500') {
            steps {
                sh 'sudo python3 -u /home/ubuntu/Signal-Detection/sp500.py'
            }
        }
    }
}
