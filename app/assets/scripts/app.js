(function() {

    var app = angular.module('app', []);
    
    app.controller("importController",['$scope','$http', '$interval', function ($scope, $http, $interval) {

        $scope.files = []

        $http.get('/listFiles').
        success(
            function(result) {
                $scope.files = result
            }
        )

        function generateTableName(fileName) {
            return "cm_test_load_" + fileName.replace(".dbf","")
        }

        $scope.importData= function(fileName) {

            var tableName = generateTableName(fileName)
            for (var i = 0; i < $scope.files.length; i++) {
                if ($scope.files[i].name === fileName)
                    $scope.files[i].tableName = tableName;
            }

            $http.post('/startImport',{fileName: fileName, tableName: tableName})
        }
        
        $interval(
            function() {
                $http.get('/progress').
                success(
                    function(result) {
                        for (var i = 0; i < $scope.files.length; i++) {
                            var progressInfo = result[$scope.files[i].name]

                            if (typeof progressInfo != 'undefined') {
                                $scope.files[i].currentCount = progressInfo.count
                                $scope.files[i].count = progressInfo.cntAll
                                $scope.files[i].dt = progressInfo.dt
                                $scope.files[i].dt_end = progressInfo.dt_end
                                $scope.files[i].dt_start = progressInfo.dt_start
                                $scope.files[i].table_name = progressInfo.table_name
                                $scope.files[i].message = progressInfo.message
                            }
                        }
                    }
                )
            },
            5000
        )

    }])
}());