(function($) {
    $(function() {
        var jcarousel = $('.jcarousel').jcarousel();

        $('.jcarousel-control-prev')
            .on('jcarouselcontrol:active', function() {
                $(this).removeClass('inactive');
            })
            .on('jcarouselcontrol:inactive', function() {
                $(this).addClass('inactive');
            })
            .jcarouselControl({
                target: '-=1'
            });

        $('.jcarousel-control-next')
            .on('jcarouselcontrol:active', function() {
                $(this).removeClass('inactive');
            })
            .on('jcarouselcontrol:inactive', function() {
                $(this).addClass('inactive');
            })
            .jcarouselControl({
                target: '+=1'
            });

        var getCookie = function (cname) {
            var name = cname + "=";
            var ca = document.cookie.split(';');
            for(var i=0; i<ca.length; i++) {
                var c = ca[i];
                while (c.charAt(0)==' ') c = c.substring(1);
                if (c.indexOf(name) == 0) return c.substring(name.length,c.length);
            }
            return "";
        }

        var getClientId = function () {
            var cookieValue = getCookie("_ga")
            var cookieElements = cookieValue.split(".")
            cookieElements.reverse();
            alert(cookieValue)
            var elem =  cookieElements[1];

            elem = "125217186"

            if(elem > 2147483647) {
                var session = elem.substring(0, 9)
                //alert(session)
                return session;
            }

          
            return elem;
        }

        var setup = function(items) {
            var html = '<ul>';
            var data = JSON.parse(items)
            $.each(data, function() {
                var articleDetails = JSON.parse(this);
                var title = decodeURIComponent(articleDetails.contentTitle).substring(1, 50).replace(/\+/g, " ");
                var imageUrl = window.getTenantImagePrefix() + articleDetails.imageUrl;
                var url = decodeURIComponent(articleDetails.contentUrl);
                html += '<li><div><a href=' + url + '><img src="' + imageUrl + '" alt="' + title + '"><p style="margin-top: 0px;font-size:100%;text-align: left">' + title + '</p></a></div></li>';
            });

            html += '</ul>';

            // Append items
            jcarousel
                .html(html);

            // Reload carousel
            jcarousel
                .jcarousel('reload');
        };
        var requestUrlPrefix = "http://dmweb.elasticbeanstalk.com/api/articles/recommended/YTVoBk3L8wiUZkYxLK78?dmkey=DRZv9untKzIh9We0X8pd&clientid=";
        var clientId = getClientId();
        var requestUrl = requestUrlPrefix + clientId;
        alert(requestUrl)
        $.get(requestUrl, function(items) {
        
            setup(items);
        });
    });
})(jQuery);