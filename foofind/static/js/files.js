if(window != top)
    top.location.href = location.href;

//si llega una busqueda ajax y no esta activado redirecciona solo desde el index
//if(window.location.pathname.substr(4,11)=="" && window.location.hash.charAt(1)=="!")
//    window.location.href=window.location.pathname.substr(1,3)+"/"+"search/?alt=ajax&"+window.location.hash.substring(3)

var lang=window.location.pathname.substr(3,1)=="/"?window.location.pathname.substr(0,4):"/";
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
$(function()
{
    //añadir el setlang para cambiar el idioma
    $('#select_language_box a').click(function(){$(this).attr("href",$(this).attr("href")+"?setlang="+$(this).attr("href").substr(1,2))});
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
