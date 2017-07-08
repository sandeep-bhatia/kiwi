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
            datomataClientId = "articleid=" + that.ArticleId;
            contextSrc = 'similiar';
        } 

        if(that.Context === 'interest') {
            contextSrc = 'taste';
        }   
         
        if(that.Context === 'recommended') {
            contextSrc = 'user-user'
        }    

        var url = "http://dmweb.elasticbeanstalk.com/api/articles/" + that.Context + "/" + that.TenantId + "?" + datomataClientId + "&dmkey=DRZv9untKzIh9We0X8pd&utm_campaign=datomata&utm_medium=" + contextSrc + "&utm_source=dmweb";
        return url
    },
    renderWidget:  function() {
        var that = this;

        $.get(that.getDatomataApiUrl(that.TenantId, that.ArticleId), function(res, status){
            var contentTitle_2 = "";
            var contentUrl_2 = "";
                var data = (JSON.parse(res));
                for(data_id in data) {
                    var obj = JSON.parse(data[data_id]);
                        var contentUrl = decodeURIComponent(obj["contentUrl"]);
                        var contentShortDesc = decodeURIComponent(obj["contentShortDesc"]);
                        var contentTitle = decodeURIComponent(obj["contentTitle"].replace(/\+/g, '%20'));
                        var contentShortDesc = decodeURIComponent(obj["contentShortDesc"].replace(/\+/g, '%20'));
                        var extras = decodeURIComponent(obj["extras"]);
                        var imageUrl = decodeURIComponent(obj["imageUrl"]);
                        
                        var createdDate = new Date(obj["createdDate"]);
                        createdDate = createdDate.getDate()+"/"+(createdDate.getMonth()+1)+"/"+createdDate.getFullYear();
                        var li_data='<li > \
                                        <div class="product_div"  ><div class="me"><div class="slider_side_div"><div class="slider_img"> \
                                            <a href="'+contentUrl+'?utm_campaign=datomata&utm_medium=click&utm_source=dmweb">\
                                                <img src="'+imageUrl+'" alt="" />\
                                            </a>\
                                            </div>\
                                            <a href="'+contentUrl+'?utm_campaign=datomata&utm_medium=click&utm_source=dmweb">\
                                            <span >'+contentTitle+'<div class="clear"></div><font><i class="fa fa-clock-o"></i>'+createdDate+'</font></span>\
                                            </a>\
                                            </div>\
                                            </div>\
                                        </div>\
                                        </li>';
                        $(".slides").append(li_data);
                    
            }
            that.slider_init();      
        });
    },
    myScroll: null,
    slider_init: function () {
        myScroll = new IScroll('#wrapper', { mouseWheel: true,  click: true });            
        var elements = document.getElementsByClassName('me')
        for(var i = 0; i<elements.length; i++)
        {
            elements[i].addEventListener('touchstart', function(){
            this.style.background = '#eee';
            }, false);
        }
        for(var i = 0; i<elements.length; i++)
        {
            elements[i].addEventListener('touchend', function(){
            this.style.background = 'transparent';
            }, false);
        }
    }
};
