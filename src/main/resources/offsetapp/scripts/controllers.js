angular.module('offsetapp.controllers',["offsetapp.services"])
	.controller("GroupCtrl", ["$scope", "$interval", "$routeParams", "offsetinfo",
							  function($scope, $interval, $routeParams, offsetinfo) {
								  offsetinfo.getGroup($routeParams.group, function(d) {
									  $scope.info = d;
									  $scope.loading=false;
								  });
								  $scope.loading=true;

								  $scope.group = $routeParams.group;
							  }])
	.controller("GroupListCtrl", ["$scope", "offsetinfo",
								  function($scope, offsetinfo) {
									  $scope.loading = true;
									  offsetinfo.listGroup().success(function(d) {
										  $scope.loading=false;
										  $scope.groups = d;
									  });
								  }])
    .controller("TopicListCtrl", ["$scope", "offsetinfo",
                                  function($scope, offsetinfo) {
                                      $scope.loading = true;
                                      offsetinfo.listTopics().success(function(d) {
                                          $scope.loading=false;
                                          $scope.topics = d;
                                      });
                                  }])
	.controller("TopicDetailCtrl", ["$scope", "$interval", "$routeParams", "offsetinfo",
    							  function($scope, $interval, $routeParams, offsetinfo) {
    								  offsetinfo.topicDetail($routeParams.group, function(d) {
    									  $scope.info = d;
    									  $scope.loading=false;
    								  });
    								  $scope.loading=true;

    								  $scope.group = $routeParams.group;
    							  }])
    .controller("ClusterVizCtrl", ["$scope", "offsetinfo",
       								  function($scope, offsetinfo) {
       									// no op
       									$scope.loading = true;
       									loadViz("#kafka-cluster", "/clusterlist")
       								  }])
    .controller("ActiveTopicsVizCtrl", ["$scope", "offsetinfo",
                                          function($scope, offsetinfo) {
                                            // no op
                                            $scope.loading = true;
                                            loadViz("#kafka-cluster", "/activetopics")
                                          }])
	.controller("TopicCtrl", ["$scope", "$routeParams", "offsetinfo",
							  function($scope, $routeParams, offsetinfo) {
								  $scope.group = $routeParams.group;
								  $scope.topic = $routeParams.topic;
								  $scope.data = [];
								  offsetinfo.getTopic($routeParams.group, $routeParams.topic, function(d) {
									  $scope.data = d.offsets;
								  });
							  }]);
