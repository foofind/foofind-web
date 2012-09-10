if(window != top)
    top.location.href = location.href;

//si llega una busqueda ajax y no esta activado redirecciona solo desde el index
if(window.location.pathname.substr(4,11)=="" && window.location.hash.charAt(1)=="!")
    window.location.href=window.location.pathname.substr(1,3)+"/"+"search/?alt=ajax&"+window.location.hash.substring(3)

var filtros={"q":""};
var imgs=0;
var jimage, jimagecount, thumbani = 0;
function animateImage()
{
    var src=jimage.attr("src");
    idx=parseInt(src.substr(src.length-1));
    idx=(idx+1)%jimagecount;
    src=src.substr(0,src.length-1) + idx.toString();
    jimage.attr("src", src);
}
function vote(obj,data)
{
    var padre=$(obj).parent();
    padre.find(".vote_up strong").text(data['c'][0]);
    padre.find(".vote_down strong").text(data['c'][1]);
    if($(obj).attr('class').search('vote_up')!=-1)
        padre.removeClass("downactive").addClass("upactive");
    else
        padre.removeClass("upactive").addClass("downactive");
}
function highlight(data,t)
{
    var i=0, a=data.toLowerCase(), b=t.toLowerCase();
    while(i<t.length && a[i]==b[i])
        i++;
    return data.substr(0,i)+"<strong>"+data.substr(i)+"</strong>"
}
$(function()
{
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
            source:form.substr(0,(form.indexOf('?')!=-1)?form.indexOf('?'):form.length)+"/autocomplete?t="+$("#type").val(),
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
    //mostrar y ocultar aviso idioma
    if(!document.cookie.match("langtest=0"))
        $("#advice").fadeIn();

    $(".close_advice").click(function(e)
    {
        document.cookie = "langtest=0";
        $(this).parent().slideUp();
        e.preventDefault();
    });
    //thumbnails
    $('.thumblink span img').mouseenter(function()
    {
        if (thumbani!=0)
            clearInterval(thumbani);

        jimage=$(this);
        imgs=jimage.attr("class").substr(4).split("_");
        jimagecount=imgs.length;
        thumbani = setInterval(animateImage, 500);
    });
    $('.thumblink span img').mouseleave(function()
    {
        if (thumbani!=0)
            clearInterval(thumbani);
    });
    $('.thumblink span img').each(function()
    {
        icount = imgs.length;
        src = $(this).attr('src').slice(0,-1);
        for (i=0; i<icount; i++)
            $('<img/>')[0].src = src+i.toString();
    });
    //search
    $("#advsearch>a").click(function(event)
    {
        event.preventDefault();
        $("#advsearch div").slideToggle();
        if($("span",this).text()=="▶")
            $("span",this).text("▼");
        else
            $("span",this).text("▶")
    });
    if($(location).attr('href').search("size|brate|year")>0)
        $("#advsearch>a").click();
    //download
    $(".download_source input").click(function(){$(this).select()});
    $('.file_comment_vote a').click(function(event)
    {
        if($(this).hasClass("vote_login"))
        {
            event.preventDefault();
            $.ajax({
                dataType:"json",
                url:$(this).attr("href"),
                context:this,
                success:function(data){vote($(this),data)}
            })
        }
    });
    $('.file_download_vote a').click(function(event)
    {
        if($(this).hasClass("vote_login"))
        {
            event.preventDefault();
            $.ajax({
                dataType:"json",
                url:$(this).attr("href"),
                context:this,
                success:function(data){vote(this,data)}
            });
        }
    });
    //translate
    $("#translate #lang").change(function(){$("form:first").submit()});
    var controls = $('textarea,input[type=text]:gt(0)');
    var empties = controls.filter("[value='']");
    $("#searchempty").click(function(event)
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
    $("textarea,input[type=text]").change(function()
    {
        empties = controls.filter("[value='']");
        $("#searchempty").html(Math.round(10000-10000*empties.size()/controls.size())/100+'% &darr;');
    });
});
$(window).unload(function(){$(":input[disabled=disabled]").removeAttr("disabled")})
