if(window != top)
    top.location.href = location.href;

String.prototype.endsWith = function(suffix) {
    return this.indexOf(suffix, this.length-suffix.length)!==-1;
}

window.lang=window.location.pathname.substr(3,1)=="/"?window.location.pathname.substr(0,4):"/en/";
window.filtros={};

function highlight(data,t)
{
    var i=0, a=data.toLowerCase(), b=t.toLowerCase();
    while(i<t.length && a[i]==b[i])
        i++;
    return data.substr(0,i)+"<strong>"+data.substr(i)+"</strong>"
}
function error(message)
{
    return '<p class="error">'+message.responseText+'</p>'
}
//comprueba sincronamente si hay un token valido para la proteccion csrf y sino lo genera y lo guarda donde corresponde
function status()
{
    $.ajax({url:"/status",type:"get",async:false}).done(function(r)
    {
        if(r!="")
        {
            csrf_token=r;
            $("_csrf_token").val(r);
        }
    })
}
//muestra cuadro de dialogo
window.modal_dialog = {
    initialized:false,
    initialize:function(){
        var me=this;
        this.element = $("#dialog");
        this.element.html(
            '<div class="outer"><div class="inner"><header></header><section></section>'
                + '<footer>'
                    + '<button class="button dialog_ok">'
                        + this.element.data("dialog_ok")
                        + '</button>'
                    + '<button class="button dialog_no">'
                        + this.element.data("dialog_no")
                        + '</button>'
                    + '<button class="button dialog_yes">'
                        + this.element.data("dialog_yes")
                        + '</button>'
                + '</footer></div></div>')
            .click(function(){me.hide.apply(me);});
        $(".outer", this.element)
            .click(function(event){
                event.preventDefault();
                event.stopPropagation();
                });
        $(".dialog_ok", this.element)
            .click(function(event){
                me.hide.apply(me);
                return me.ok_callback.apply(me, [event]);
                });
        $(".dialog_yes", this.element)
            .click(function(event){
                me.hide.apply(me);
                return me.yes_callback.apply(me, [event]);
                });
        $(".dialog_no", this.element)
            .click(function(event){
                me.hide.apply(me);
                return me.no_callback.apply(me, [event]);
                });
        this.initialized = true;
        },
    element:null,
    show:function(options){
        /* Opciones (objeto:
         *  mode:
         *  title:
         *  text:
         *  yes:
         *  no:
         *  ok:
         *  ok_callback
         *  yes_callback:
         *  no_callback:
         */
        if(!this.initialized) this.initialize();

        var simple=!(options.yes||options.no||options.yes_callback||options.no_callback);
        $(".dialog_ok", this.element).css("display", (simple?"auto":"none"));
        $(".dialog_yes", this.element).css("display", (simple?"none":"auto"));
        $(".dialog_no", this.element).css("display", (simple?"none":"auto"));

        $("header", this.element).html(options.title||"").css("display", "auto");
        if(!options.title) $("header", this.element).css("display", "none");

        $("section", this.element).html(options.text||"");

        $(".dialog_ok", this.element).html(options.ok||this.element.data("dialog_ok"));
        $(".dialog_yes", this.element).html(options.yes||this.element.data("dialog_yes"));
        $(".dialog_no", this.element).html(options.no||this.element.data("dialog_no"));

        this.ok_callback = options.ok_callback||function(){};
        this.yes_callback = options.yes_callback||function(){};
        this.no_callback = options.no_callback||function(){};

        this.element.removeClass();
        if(options.mode) this.element.addClass(options.mode);

        this.element.css("opacity", 0);
        this.element.css("display", "auto");
        this.element.fadeTo(250, 1);
        },
    hide:function(){
        if(this.element&&(this.element.css("display")!="none")){
            var me=this;
            this.element.fadeTo(250, 0, function(){me.element.css("display", "none");});
            }
        }
    };

window.downloader = {
    expiration_days:365,
    initialized:false,
    skip:false,
    initialize:function(){
        this.skip = (document.cookie.indexOf("skip_downloader=1") > -1);
        this.initialized = true;
        },
    disable:function(){
        if(!this.skip){
            var expiration=new Date();
            expiration.setDate(expiration.getDate() + this.expiration_days);
            document.cookie = "skip_downloader=1; expires=" + expiration.toUTCString() + "; path=/";
            this.skip = true;
            }
        },
    proxy:function(url, target){
        var me=this, downloader=$("body").data("downloader_href");
        _gaq.push(['_trackEvent', "FDM", "offer"]);
        window.modal_dialog.show({
            mode: "downloader",
            title: $("body").data("downloader_title"),
            text: $("body").data("downloader_text"),
            yes: $("body").data("downloader_yes"),
            no: $("body").data("downloader_no"),
            yes_callback: function(){
                _gaq.push(['_trackEvent', "FDM", "offer accepted"]);
                me.disable();
                setTimeout(function(){window.location.href = downloader}, 100);
                },
            no_callback: function(){
                _gaq.push(['_trackEvent', "FDM", "offer rejected"]);
                me.disable();
                if(target=="_blank") window.open(url);
                else setTimeout(function(){window.location.href = url}, 100);
                }
            });
        },
    link_lookup:function(parent){
        if(!this.initialized) this.initialize();
        if(!this.skip){
            var me=this, url, target, cback=function(){document.location.href = url;};
            $("a", parent).each(function(i){
                var elm=$(this), url=this.href, target=this.target;
                if(elm.data("downloader")=="1")
                    elm.click(function(event){
                        if(me.skip) return;
                        event.stop_redirection = true; // Usado por link_stats
                        me.proxy.apply(me, [url, target]);
                        event.preventDefault();
                        });
                });
            }
        }
    };

$(function()
{
    //añadir el setlang para cambiar el idioma
    $('#select_language_box a').click(function(){$(this).attr("href",$(this).attr("href")+"?setlang="+$(this).attr("href").substr(1,2)+window.location.hash)});
    //mantener viva la sesion de forma ilimitada mientras se tenga la pagina abierta
    setInterval(status,1000000);
    //autocompletado de busqueda
    var form=$('form[method="get"]').attr("action");
    if($('#q').length)
    {
        $('#q').focus()
        .focus(function(){$("#params").addClass("hover")})
        .keypress(function(){$("#params").addClass("hover")})
        .blur(function(){$("#params").removeClass("hover")})
        .autocomplete(
        {
            source:lang+"autocomplete?t="+$("#type").val(),
            select:function(event,ui)
            {
                if(ui.item)
                    $(this).val(ui.item.value);

                $(this).parents("form").submit();
            },
            open: function(){$(this).autocomplete('widget').css('z-index',11)},
            appendTo:($('#search').length)?'#search':'body',
            delay:0,
            minLength: 2,
            disabled: autocomplete_disabled
        })
        .data("autocomplete")
        ._renderItem=function(ul,item){return $("<li></li>").data("item.autocomplete",item ).append("<a>"+highlight(item.label,this.term)+"</a>").appendTo(ul)}
        $("#params>a").click(function(){$("#q").focus()});
        $("#home #type:not(:hidden)").selectmenu(
        {
            style:"popup",
            width:239,
            maxHeight:300,
            icons:[{find:'li'}],
            open:function(){$("#params").addClass("hover")},
            close:function(){$("#params").removeClass("hover")},
            change:function(){$('#q').autocomplete("option","source",form.substr(0,(form.indexOf('?')!=-1)?form.indexOf('?'):form.length)+"/autocomplete?t="+$("#type").val())}
        });
    }
    //campos en blanco
    $('#home form,.searchbox').submit(function(e)
    {
        if(!$("#q").val()) //si no hay nada para buscar
        {
            $("#q").attr("placeholder",$("#search_submit").data("no_query"));
            e.preventDefault();
        }
        else
            $.each($(":input",this),function()
            {
                if(filtros["q"]=="" && (!$(this).val() || $(this).attr("type")=="submit"))
                    $(this).attr("disabled","disabled")
            })
    })
    //mostrar aviso idioma
    if(!document.cookie.match("langtest=0"))
        $("#beta_lang").show(0);
    //ocultar cualquier aviso
    $(".advice button").click(function(e)
    {
        $(this).parent().slideUp();
        if($(this).parent().attr("id")=="beta_lang") //para los avisos de idioma en beta se guarda la cookie para que no salga de nuevo
            document.cookie="langtest=0";
    });
    //translate
    $("#translate #lang").change(function(){$("form:first").submit()});
    var controls = $('textarea,input[type=text]:gt(0)');
    var empties = controls.filter("[value='']");
    $("#translate #searchempty").click(function(event)
    {
        event.preventDefault();
        var offset, docScroll, docHeight;
        docScroll = $("html").scrollTop();
        if(isNaN(docScroll))
            docScroll=0;

        $("html").scrollTop(docScroll);
        winHeight = $(window).height();
        empties = controls.filter("[value='']");
        var c;
        for(index = 0; index < empties.length; index++)
        {
            c = $(empties[index]);
            offset=c.offset().top;
            if (offset>docScroll)
            {
                if (offset > winHeight/2) offset -= winHeight/2;
                $("html, body").animate({scrollTop:offset}, 400, function() { c.focus(); });
                break;
            }
        }
        $(this).html(Math.round(10000-10000*empties.size()/controls.size())/100+'% &darr;');
    });
    $("#translate textarea,#translate input[type=text]").change(function()
    {
        empties = controls.filter("[value='']");
        $("#searchempty").html(Math.round(10000-10000*empties.size()/controls.size())/100+'% &darr;');
    });
});
$(window).unload(function(){$(":input[disabled=disabled]").removeAttr("disabled")})
