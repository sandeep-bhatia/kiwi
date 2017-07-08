$('#carousel-image-and-text').hide();

var DatomataApi =  {
    ArticleId : "",
    TenantId : "YTVoBk3L8wiUZkYxLK78",
    Context: "similiar",
    getCookie: function (cname) {
        var name = cname + "=";
        var ca = document.cookie.split(';');
        for(var i=0; i<ca.length; i++) {
            var c = ca[i];
            while (c.charAt(0)==' ') c = c.substring(1);
            if (c.indexOf(name) == 0) return c.substring(name.length,c.length);
        }
        return "";
    },
    getClientId: function () {
        var that = this;

        var cookieValue = that.getCookie("_ga")
        var cookieElements = cookieValue.split(".")
        cookieElements.reverse();
        var elem =  cookieElements[1];

        if(elem && elem > 2147483647) {
            var session = elem.substring(0, 9)
            return session;
        }

        if(!elem) {
            elem = "786"
        }

        return elem
    },
    getDatomataApiUrl: function (tenantId, articleId) {
        var that = this;
        var contextSrc = 'user-user';

        var datomataClientId = "clientid=" + that.getClientId()
        
        if(that.Context === "similar") {
            datomataClientId = "articleid=" + articleId;
            contextSrc = 'similiar';
        } 

        if(that.Context === 'interest') {
            contextSrc = 'taste';
        }   
         
        if(that.Context === 'recommended') {
            contextSrc = 'user-user'
        }    
        var url = "http://dmweb.elasticbeanstalk.com/api/articles/" + that.Context + "/" + that.TenantId + "?" + datomataClientId + "&dmkey=DRZv9untKzIh9We0X8pd&dmsource=web";
        return url
    },
    renderWidget:  function() {
        var that = this;
        var contextSrc = 'user-user';
        
        if(that.Context === "similar") {
            contextSrc = 'similiar';
        } 

        if(that.Context === 'interest') {
            contextSrc = 'taste';
        }   
         
        if(that.Context === 'recommended') {
            contextSrc = 'user-user'
        }    

        var url = that.getDatomataApiUrl(that.TenantId, that.ArticleId);
        $.get(url, function(res, status){
            var contentTitle_2 = "";
            var contentUrl_2 = "";
                var data = (JSON.parse(res));
                $('#carousel-image-and-text').show();
                for(data_id in data) {
                    var obj = JSON.parse(data[data_id]);
                    var contentUrl = decodeURIComponent(obj["contentUrl"]);
                    var contentTitle = decodeURIComponent(obj["contentTitle"].replace(/\+/g, '%20'));
                       
                    var imageUrl = decodeURIComponent(obj["imageUrl"]);

                    var createdDate = new Date(obj["createdDate"]);
                    createdDate = createdDate.getDate()+"/"+(createdDate.getMonth()+1)+"/"+createdDate.getFullYear();

                    if(window.innerWidth == 640){
                        var w_size = (window.innerWidth/4)+"px";
                    }
                    
                    if(data_id % 2 == 0 && typeof data[parseInt(data_id)+1] != 'undefined'){
                        var obj_2 = JSON.parse(data[parseInt(data_id)+1]);
                        contentTitle_2 = decodeURIComponent(obj_2["contentTitle"].replace(/\+/g, '%20'));
                        contentUrl_2 = decodeURIComponent(obj_2["contentUrl"]);
                    }else{
                        contentTitle_2 = "";
                        contentUrl_2 = "";
                    }
                    var li_data='<li class="touchcarousel-item"> \
                                        <a class="item-block" href="'+contentUrl+'?utm_campaign=datomata&utm_medium='+contextSrc+'&utm_source=dmweb">\
                                            <img src="'+imageUrl+'" alt="'+contentUrl+'" />\
                                            <div class="text"><h4>'+contentTitle+'</h4></div>\
                                        </a>\
                                        <span><i class="fa fa-clock-o"></i>'+createdDate+'</span>\
                                            </div>\
                                    </li>';                     
                        $(".touchcarousel-container").append(li_data);
                    
            }
            that.slider_init();      
        });
    },
    slider_init: function () {
        jQuery(function($) {
            $("#carousel-image-text-horizontal").touchCarousel({            
                itemsPerMove: 4,
                pagingNav: true,
                scrollbar: false,               
                scrollToLast: true,
                loopItems: true             
            });
            
            $("#carousel-single-image").touchCarousel({
                pagingNav: true,
                scrollbar: false,
                directionNavAutoHide: false,                
                itemsPerMove: 1,                
                loopItems: true,                
                directionNav: false,
                autoplay: false,
                autoplayDelay: 2000,
                useWebkit3d: true,
                transitionSpeed: 400
            });
            
            $("#carousel-image-and-text").touchCarousel({                   
                pagingNav: false,
                snapToItems: false,
                itemsPerMove: 4,                
                scrollToLast: false,
                loopItems: false,
                scrollbar: true
            });
            
            $("#carousel-gallery").touchCarousel({              
                itemsPerPage: 1,                
                scrollbar: true,
                scrollbarAutoHide: true,
                scrollbarTheme: "dark",             
                pagingNav: false,
                snapToItems: false,
                scrollToLast: true,
                useWebkit3d: true,              
                loopItems: false
            });         
        });
    }
};
