window.soft_scroller = {
    resize_check:null,
    initialize: function(){
        var a=$("footer ul.menubar a"), me=this, internal=function(l){
            // Devuelve true si la URL es de un ancla en la página actual
            return (
                l.href && (l.href.indexOf("#") > -1) && (
                    (l.pathname == location.pathname) || ('/' + l.pathname == location.pathname))
                );
            };
        a.each(function(){
            if(internal(this)){ // Ancla local
                $(this).click(function(e){
                    e.preventDefault();
                    e.stopPropagation();
                    me.scrollToAnchor(this.hash);
                    });
                }
            });
        if(a.length>0)
            $(window).resize(function(){
                if((window.location.hash)&&(me.resize_check==null))
                    me.resize_check=setTimeout(function(){
                        me.resize_check = null;
                        me.scrollToAnchor();
                        }, 100);
                });
            if(window.location.hash) this.scrollToAnchor();
            else this.activateButton("#overview");
    },
    activateButton: function(id){
        $("footer ul.menubar a").each(function(){
            var elm=$(this).parent();
            if(this.hash == id) elm.addClass("active");
            else elm.removeClass("active");
            });
        },
    scrollToAnchor: function(id){
        // Scroll al elemento dado
        var elm=$(id||window.location.hash);
        if(elm.length==0) return;
        var x=elm.position(),
            sc=$('html, body'),
            elmid="#"+elm.attr("id"),
            cbgid='bg_'+elmid.substr(1),
            cbg=$("#"+cbgid), // capa de fondo para la zona actual
            bgchange=(cbg.css("display") == "none"),
            fbg,
            t=500;

        if(bgchange)
        {
            // Obtengo el resto de capas de fondo
            fbg = $(".background").filter(function(){
                    return ((this.style.display!="none")&&(this.id!=cbgid));});
            cbg.stop();
            fbg.stop();
        }

        sc.stop();
        this.activateButton(elmid);

        if(elmid != window.location.hash)
        {
            sc.animate(
                {scrollLeft:x.left, scrollTop:x.top},
                {duration:t, complete:function(){window.location.hash = elm.attr("id");}}
                );
            if(bgchange){
                cbg.css("opacity", 0).css("left", fbg.css("left")).css("display","block");
                cbg.animate(
                    {opacity:1, left:-x.left/2},
                    {duration:t}
                    );
                fbg.animate(
                    {opacity:0, left:-x.left/2},
                    {duration:t, complete:function(){fbg.css("display", "none");}}
                    );
                }
        }
        else
        {
            sc.scrollLeft(x.left).scrollTop(x.top);
            cbg.css("left", -x.left/2);
            if(bgchange){
                cbg.css("display", "block");
                fbg.css("display", "none");
            }
        }
    }
}

$(document).ready(function(){
    var qi=window.location.href.indexOf("?"),
        mode=(qi>-1)?window.location.href.substr(qi):"?a=0"+window.location.hash;
    soft_scroller.initialize();
    $("#download_button")
        .click(function(e){
            _gaq.push(['_trackEvent','FDM', "Download", "Microsite - " + mode + window.location.hash]);
            e.preventDefault();
            setTimeout('document.location = "'+this.href+'"',100);
            });
    });
