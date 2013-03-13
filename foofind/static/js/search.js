var cargando,cargando150;
window.first_download=window.current_url=window.current_url_info=false;
var pagina=devueltos=0; //necesario ponerlo tambien aqui para que funcione la paginacion de la primera busqueda
var MAX_RESULTS = 10000;
var last_items="";
var archivos={};
var peticiones={},num_peticiones=0;
var alto,ancho,arriba,subcontent_top,ancho_max_download=1440,ancho_min_download=930,posicion_flecha=610; //TODO min-width 940
var respuesta_total_found=-1, last_wait=500;
var busqueda_programada=espera=0;
var hash=checked=false;
var current_search_page = 0;

function link_lookup(parent){
    // busqueda de enlaces de downloader (data-downloader)
    if($("body").data("downloader_text"))
        window.downloader.link_lookup(parent);

    // enlaces estadísticos (data-link_stats)
    $("a", parent).each(function(){
        var elm=$(this), link_href=this.href, target=this.target;
        if(elm.data("stats")&&!elm.data("link_stats_handled")){
            var data=elm.data("stats").split(";"), wait=elm.attr("_target");
            elm.data("link_stats_handled", true);
            elm.click(function(event){
                _gaq.push(['_trackEvent', data[0], data[1], data[2]]);
                if(!target){
                    setTimeout(function(){
                        if(!event.stop_redirection) window.location = link_href;
                        }, 100);
                    event.preventDefault();
                    }
                });
            }
        });
    }

//obtiene la cadena de busqueda con o sin paginación
function search_string(paginacion)
{
    var filtro="",add_slash=false;
    if(!filtros["q"])  //no se puede usar la busqueda si no hay nada para buscar
        if($("#q").val()) //si es download sin busqueda se utiliza lo que viene en el nombre
            filtros["q"]=$("#q").val();
        else //sino se redirige al index
            window.location="/";

    filtro=encodeURIComponent(filtros["q"].replace("/","_")).replace(/%20/g,"_");
    order=["src","type","size"];
    for(var clave in order)
        if(order[clave] in filtros)
        {
            add_slash=true;
            filtro+="/"+order[clave]+":"+filtros[order[clave]];
        }

    if(add_slash)
        filtro+="/";

    if(paginacion)
        return filtro+"&last_items="+last_items;
    else
        return filtro;
}

//extraccion de los filtros de busqueda y actualizacion de la interfaz
function search_params()
{
    filtros={};
    if (current_url_info.has_search) jQuery.extend(filtros, current_url_info.search);
    if (current_url_info.has_download) jQuery.extend(filtros, current_url_info.download);

    //colocar la informacion y activar botones
    $('#src>ul>li, #type li').removeClass("active");
    if("q" in filtros)
        $("#q").val(filtros["q"]);

    if("type" in filtros)
    {
        var tipos=filtros["type"].split(",");
        for(var type in tipos)
            $("#type a[data-filter="+tipos[type]+"]").parent("li").addClass("active");
    }
    if("src" in filtros)
    {

        var sources=filtros["src"].split(",");
        for(var src in sources)
            $('#src a[data-filter="'+sources[src]+'"]').parents("li").addClass("active");
    }
    if("size" in filtros)
        if(filtros["size"].indexOf(",")==-1) //si viene de buscar un tamaño en la busqueda clasica se ignora
            delete(filtros["size"]);
        else
            $("#size div").slider("values",filtros["size"].split(","));
    else
        $("#size div").slider("values",[17,34]);
}
//hace una recarga de la pagina
function page_reload()
{
    parar_peticiones(true,true);
    //limpiar la interfaz
    $("#search_info").empty();
    $("#more").remove();
    $("#results").html(rotar(cargando,true));
    ocultar(0,true,true); //aqui es donde se recarga la pagina al mandar el tercer parametro a true
}
//cambia la URL de busqueda mediante el history de HTML5, con hashbang (#!) o para hacer una redireccion
function change_url(change_download)
{
    var must_track = true;
    var has_changed = false;

    //guarda la url nueva para compararla con la actual y cargarla si no coincide TODO arreglar cuando no viene dn
    var target_info = clone(current_url_info);
    var target_url;

    // genera la info para generar la url nueva segun sea cambio de busqueda o de download
    if(change_download) {
        target_info.has_search = current_url_info.has_search || !window.history.pushState;
        target_info.has_download = "d" in filtros;
        if (target_info.has_download)
        {
            has_changed = (current_url_info.has_search!=target_info.has_search) || !current_url_info.has_download || (filtros["d"]!=current_url_info.download["d"]);
            target_info.download = {"d":filtros["d"]}
            if ("dn" in filtros)
                target_info.download["dn"] = filtros["dn"];
        } else {
            has_changed = current_url_info.has_download;
        }
    } else {
        search_filters = clone(filtros);
        delete search_filters["d"];
        delete search_filters["dn"];

        target_info.has_search = true;
        target_info.has_download = false;

        target_info.search = search_filters;
        has_changed = true; // si ha cambiado se mira despues
    }

    // genera la url nueva y mira si es diferente a la actual
    target_url = format_url(target_info);
    if (target_url==current_url) {
        has_changed = false;
    }

    // si no ha cambiado la url sale
    if (!has_changed) return;

    // navegar con hashbang (busqueda o desde download sin busqueda en navegador sin soporte para history)
    if (!window.history.pushState && (!change_download || !current_url_info.has_search))
        window.location = format_url(target_info, true);
    // navegar con url normal (busqueda en navegador con soporte para history)
    else if (!change_download)
        window.location = target_url;

    // cambio de url sin navegación
    else {

        if(window.history.pushState) //si el navegador soporta history
            window.history.pushState(filtros,"",target_url);
        else {
            var old_title = document.title;
            //sino se usa hashbang
            window.location.hash=format_url(target_info, false); // genera la parte del hash con hashbang
            document.title = old_title;
        }

        // actualiza información de url actual y registra visita
        update_current_url();
        track_pageview();

        //actualizar los enlaces de cambio de idioma
        $('#select_language_box a[href]').not(":last").each(function(){$(this).attr("href","/"+$(this).attr("hreflang")+window.location.pathname.substr(3))});
    }
}

//realiza la busqueda con los filtros actuales
function search(paginacion)
{
    var results=$("#results");
    results.append(rotar(cargando,true)); //se ponen ojitos
    $("#more").fadeOut("fast");
    paginacion=(paginacion===true); //puede ser info de un evento
    var espera_actual=espera-new Date().getTime(); //diferencia entre la fecha de la siguiente busqueda permitida y la fecha actual
    if(espera_actual>0) //si hay que esperar se programa la siguiente busqueda
    {
        busqueda_programada=setTimeout(function(){espera=0;search(paginacion)},espera_actual); //se tiene que poner espera a 0 para que entre a buscar
        espera=-1;
        return;
    }
    else if(espera==-1) //para que no busque si se vuelve a entrar aqui y esta activo el timeout
        return;
    else //solo se permiten busquedas programadas
        espera=-1;

    var peticion=paginacion?"paginacion":"busqueda";
    peticiones[peticion]=$.ajax({url:lang+"searcha",data:"filters="+search_string(paginacion)}).done(function(respuesta)
    {
        espera=new Date().getTime();
        if(respuesta)
        {
            num_peticiones[peticion+"_total"]=0; //reiniciar el contador de peticiones
            pagina=Math.max(pagina,respuesta["page"]);
            last_items=respuesta["last_items"];

            wait=respuesta["wait"];
            if (wait>0) last_wait = wait;
            espera += last_wait; //fecha en milisegundos de la proxima busqueda permitida

            sure = respuesta["sure"];
            respuesta_total_found=respuesta["total_found"];

            //si hay resultados o ya no quedan resultados parar ojitos
            if(respuesta["files_ids"].length || pagina>=respuesta_total_found || pagina>=MAX_RESULTS)
                cargando=rotar(cargando,false);

            var respdata;
            for(var resp in respuesta["files_ids"]) //añadir resultado si no estaba ya antes
            {
                if(!results.find('>li>div>h3>a[data-id="'+respuesta["files_ids"][resp]+'"]').length) //evitar repetidos
                {
                    devueltos++;
                    respdata = $(respuesta["files"][resp]).appendTo(results);

                    //revisa los enlaces
                    link_lookup(respdata);

                    //precarga de miniaturas
                    var result=respdata.find(".thumblink img");
                    if(result.data("servers"))
                        for(var index in result.data("servers").split("_"))
                            $('<img>').attr("src",result.attr('src').slice(0,-1)+index);
                }
            }

            more_results=load_more_results(sure);

            // Si hay resultados o se está seguro de que no hay se muestra el número de resultados
            if (respuesta_total_found>0 || sure) {
                if (respuesta_total_found==0) $("#loading").empty();
                $("#search_info").html(respuesta["result_number"]);
            }

            if(more_results==2)
                $("#more").css("display","block");

            //si es la primera pagina y en la URL venia un archivo se intenta mostrar directamente
            if("d" in filtros && !$("#download").hasClass("active"))
                mostrar($('#results h3 a[data-id="'+filtros["d"]+'"]'));

        }
        else //si hay un error en el servidor se vuelve a pedir
        {
            espera+=100;
            search(paginacion);
        }

    }).fail(function(req,status)
    {
        if (status=="abort") //evita seguir si se ha parado la petición voluntariamente
            return;
        else if(num_peticiones>=10) //evitar bucles de peticiones cuando hay algun fallo
        {
            cargando=rotar(cargando,false); //parar ojitos
            $("#more").css("display","block");
            num_peticiones=0;
        }
        else //hasta que no se llega al limite se busca
        {
            $("#more").css("display","none");
            num_peticiones++;
            espera=new Date().getTime()+Math.log(num_peticiones)*1000;
            search(paginacion);
        }
    });
}
//carga mas resultados de busqueda si es necesario
function load_more_results(sure)
{
    if(pagina<MAX_RESULTS && (!sure || pagina<respuesta_total_found)) //si hay mas paginas para cargar
    {
        //si ya estan puestos todos los resultados que se han devuelto y la altura de los mismos es menor de lo esperado se busca
        if($('#results>li[id!=loading]').length==devueltos && arriba>$("#results").height()-104*(arriba==0?3:5)-alto)
        {
            search(true);
            $("#more").css("display","none");
            return 1;
        }
        else
            return 2;
    }
    return -1;
}

//detiene las peticiones ajax de busqueda, archivo o paginacion
function parar_peticiones(paginacion,siempre)
{
    for(peticion in peticiones)
        if((paginacion || peticion=="descarga") && peticiones[peticion] && peticiones[peticion].readyState!=4)
            peticiones[peticion].abort();

    if(siempre) //parar todas las peticiones que pudieran pasar en un futuro
    {
        espera=-1;
        clearTimeout(busqueda_programada);
    }
}

//muestra la ventana de download
function mostrar(file,ocul)
{
    //colapsa o expande los metadatos y comentarios del archivo
    function show_metadata_comments(download)
    {
        download.find("#metadata").addClass(function(i,val){return $(this).height()>150?"active":""});
        download.find("#comments").addClass(function(i,val){return $(this).height()>300?"active":""});
    }

    //para corregir el error cuando se llama desde el hoverIntent de download y no hay li.active o si hay alguna animacion pendiente
    if(!file.length || $(":animated").length)
        return;

    var download=$("#download");
    var id=filtros["d"]=file.data("id");
    if(!filtros["q"])
        filtros["dn"]=file.attr("title");

    var file=file.parents("li");
    var play=$(".thumblink",file);
    update_global_ui_vars();
    change_url(true);
    if(!$("#fondo").data("visible") || play.data("active_play")) //se carga el download si no esta el fondo oscuro o hay que reproducir
    {
        if($(">article",download).data("id")!=id || play.data("active_play")) //si es otro distinto se carga o si hay que reproducir
        {
            if(archivos[id]) //si esta se usa
            {
                if(play.data("play"))
                    if(play.data("active_play")) //si se ha pulsado en el link de previsualizacion se activa en el embed
                    {
                        play.data("active_play",false);
                        download.html(archivos[id].replace(play.data("play")[0],play.data("play")[1]))
                    }
                    else
                        download.html(archivos[id].replace(play.data("play")[1],play.data("play")[0]))
                else
                    download.html(archivos[id])
                download.toggleClass("bottom",(download.prop("scrollHeight")>download.outerHeight()));
                show_metadata_comments(download);
                link_lookup(download);
            }
            else //sino se carga
            {
                cargando150=rotar($(cargando150).data("id",id),false); //se le añade el id para no cargarlo varias veces
                download.removeClass("active").html(rotar(cargando150,true));
                parar_peticiones(); //abortar otra peticion ajax de archivo si se estuviera enviando
                peticiones["descarga"]=$.ajax({url:lang+"downloada",data:{id:id,name:$("h3 a",file).attr("title")}}).done(function(data)
                {
                    //si no se hace antes se machaca al hacer el html en download
                    cargando150=rotar(cargando150,false);
                    if($(".thumblink",file).data("active_play")) //si se ha pulsado en el link de previsualizacion se activa en el embed
                    {
                        $(".thumblink",file).data("active_play",false);
                        data["html"]=data["html"].replace(data["play"][0],data["play"][1])
                    }
                    $(".thumblink",file).data("play",data["play"]);
                    data["html"]=data["html"].replace(/__url_share__/g,window.location.protocol+"//"+window.location.host+current_url) //actualizar url para compartir
                    if($('#results li.active h3 a').data("id")==id) //if paranoico para comprobar que se carga donde debe
                        download.html(data["html"]).addClass("active").toggleClass("bottom",(download.prop("scrollHeight")>download.outerHeight()));

                    archivos[id]=data["html"];
                    show_metadata_comments(download);
                    link_lookup(download);
                }).fail(function(data)
                {
                    if(data.readyState!=0) //necesario por si se aborta, que se ejecuta esto despues de meter los ojos
                    {
                        cargando150=rotar(cargando150,false);
                        var error=$("#errorhandler",data.responseText);
                        $("a:last",error).remove();
                        archivos[id]=download.html('<article data-id="'+id+'">'+error.html()+'</article>').html();
                    }
                    //TODO no sale el enlace de cerrar ventana
                });
            }
        }
        else //sino solo se indica que esta activo y se expanden o colapsan los datos del archivo
        {
            download.addClass("active");
            show_metadata_comments(download);
        }

        if(ancho>=ancho_max_download) //fondo gris si no estaba active a la derecha
            $("#fondo").data("visible",false);
        else
            $("#fondo").fadeTo(200,.5).data("visible",true);

        //flecha de union
        $("#subcontent").animate({width:ancho-20},{duration:200,queue:false});
        $("#flecha").css({top:file.position().top}).show(100)
        .animate({right:ancho>ancho_max_download+60?posicion_flecha+50:ancho>ancho_max_download?posicion_flecha+ancho-ancho_max_download-20:ancho<ancho_min_download?posicion_flecha+10:posicion_flecha-20},{duration:200,queue:false});
        //quitar otro resultado que estuviera activo y activar el actual
        $("#results li").removeClass("active").css("z-index",0);
        file.addClass("active").css("z-index",5);
        //como esta en position fixed se limita la posicion ya que no se puede hacer por css
        download.css({position:"fixed",top:arriba<subcontent_top?subcontent_top-arriba:0,height:alto-55}).show(100)
        .animate({right:ancho>ancho_max_download?ancho-ancho_max_download:ancho<ancho_min_download?ancho-ancho_min_download:0},{duration:200,queue:false});

    }
    else if(ocul)
        ocultar();
}
//oculta la ventana de download
function ocultar(duracion,derecha,cambiar)
{
    if(typeof(duracion)!="number")
        duracion=200;

    $("#fondo").fadeOut(duracion).data("visible",false);
    if(!$("#results:empty").length && !derecha) //dejar a la derecha de los resultados si no es una busqueda
    {
        $("#subcontent").animate({"width":ancho_max_download},{duration:duracion,queue:false});
        $("#flecha").animate({right:posicion_flecha},{duration:duracion,queue:false}); //necesario por poner el subcontent -20 de ancho
        $("#download").css({position:"fixed",top:arriba<subcontent_top?subcontent_top-arriba:0}).animate({right:ancho-ancho_max_download},{duration:duracion,queue:false});
    }
    else //ocultar a la derecha completamente
    {
        cargando150=rotar(cargando150,false);
        parar_peticiones();
        $("#results li").removeClass("active");
        $("#flecha").hide(duracion).animate({right:-125},{duration:duracion,queue:false});//,complete:function(){$(this).show()}});
        $("#download").hide(duracion).animate({right:-780},{duration:duracion,queue:false,complete:function(){$(this).removeClass("active").empty()}});
        delete filtros["d"];
        delete filtros["dn"];
        change_url(!cambiar);
    }
}
//rota los iconos de carga
function rotar(selector,inicio)
{
    if(inicio) //si hay que mostrarlo se rota
    {
        var rotacion=function(rot) //genera el intervalo de rotacion
        {
            return setInterval(function()
            {
                if(Math.random()<0.3)
                {
                    r='rotate('+Math.random()*359+'deg)';
                    rot.css({'-moz-transform':r,'-webkit-transform':r,'-ms-transform':r,'-o-transform':r,'transform':r})
                }
            },60);
        }
        for(var i=0;i<$(">img",selector).length;i++)
        {
            var r=$(">img:eq("+i+")",selector);
            if(r.data("rotacion")===true) //si no estan moviendose se rotan
                r.data("rotacion",rotacion(r));
        }
        return selector.show();
    }
    else //sino se para y se quita
    {
        for(var i=0;i<$(">img",selector).length;i++)
        {
            clearInterval($(">img:eq("+i+")",selector).data("rotacion"));
            $(">img:eq("+i+")",selector).data("rotacion",true); //estado inicializado
        }
        return selector.hide().detach();
    }
}
function size_slider(values,slider)
{
    var size,filesize={},i,max=$(slider).slider("option","max"),min=$(slider).slider("option","min");
    for(var j in values)
    {
        size=Math.pow(2,values[j]);
        i=parseInt(Math.floor(Math.log(size)/Math.log(1024)));
        filesize[j]=Math.round(size/Math.pow(1024,i),2)+' '+['Bytes','KiB','MiB','GiB'][i];
    }
    $("#size span").eq(0).text(values[0]==min?"min":filesize[0]);
    $("#size span").eq(1).text(values[1]==max?"max":filesize[1]);
    if(values==min+","+max) //si no hay nada especificado se borra (hace la conversion automatica de array a string)
        delete filtros["size"];
    else
        filtros["size"]=[values[0]==min?0:values[0], values[1]==max?50:values[1]];
}

$(function()
{
    update_current_url();
    subcontent_top=$("#subcontent").position().top;
    update_global_ui_vars();
    $("html").css("overflow-y","scroll");
    //inicializar los ojitos
    cargando=rotar($("#loading"),false);
    cargando150=rotar($("#download>div"),false);
    //change necesario para inicializar
    $("#size div,#quality").slider({min:17,max:34,values:[17,34],range:true,stop:page_reload,change:function(e,ui){size_slider(ui.values,this)},slide:function(e,ui){size_slider(ui.values,this)}});
    search_params();
    //envio del formulario
    $('.searchbox').submit(function(e)
    {
        e.preventDefault();

        var new_q = $("#q").val();
        if (filtros["q"]==new_q) return;

        filtros["q"]=new_q;
        $(":submit").attr("disabled","disabled")
        page_reload();
    })
    //filtros
    $('aside dd').on("click","a[href]",function(e) //TODO poner strong cuando no hay suborigenes activados y se activa uno
    {
        var id=$(this).parents("dd").attr("id");
        //activar o desactivar el li padre
        var parent=$(this).parent("li");
        var source=!parent.attr("title");
        parent.toggleClass("active");
        if(parent.hasClass("active")) //si esta activado se añade el filtro
        {
            if(source) //si es un origen principal tambien hay que activar los suborigenes
                parent.children("ul").addClass(function(i,current){return current=="inactive"?"deactive":""}).removeClass("inactive").children("li").not(":last").addClass("active");
            else //si es un suborigen se activa el origen
                parent.parent("ul").addClass(function(i,current){return current=="inactive"?"deactive":""}).removeClass("inactive").parent("li").addClass("active");
        }
        else //sino se quita del filtro
        {
            if(source) //si es un origen principal tambien hay que desactivar los suborigenes
                parent.children("ul").removeClass().addClass("deactive").children("li").removeClass("active");
            else //y si es un suborigen
            {
                var actived=parent.parent("ul").find("li.active"); //suborigenes activados
                if(!actived.length) //si no hay mas activados se desactiva el origen
                    parent.parent("ul").removeClass().addClass("deactive").parent("li").removeClass("active");
                else if(actived.length==parent.parent("ul").children("li")) //si todos estan activados
                    parent.removeClass("active");
            }
            if(!$("#"+id+">ul>li.active").length) //si es el ultimo origen se quita el filtro completo
                delete(filtros[id]);
        }
        var filter=$("#"+id+">ul>li.active"); //todos los suborigenes que estan activados
        if(filter.length)
        {
            filtros[id]="";
            filter.each(function() //recorrer los origenes
            {
                var childrens=$("ul li a",this).length; //hijos totales
                var childrens_active=$("ul li.active a",this); //hijos activos
                if(id=="type" || childrens_active.length==childrens) //si son los tipos o estan todos los suborigenes activados poner solo el origen
                    filtros[id]+=(filtros[id]?",":"")+$("a",this).data("filter");
                else //sino poner solo los suborigenes activados
                    childrens_active.each(function(){filtros[id]+=(filtros[id]?",":"")+$(this).data("filter")});
            });
        }
        if(source) //si es un origen se recarga
            page_reload();
        else //si es un suborigen se recarga en el mouseleave
        {
            checked=true;
            $(this).parents("dd").addClass("filter");
            $(this).parent("li").parent("ul").addClass("show deactive").children("li:last-child").show();
        }
        e.preventDefault();
        e.stopPropagation();
    })
    .on("click","li li",function(){$("a",this).click()})
    .on("mouseleave",">ul>li",function(){$(this).parents("dd").removeClass("filter").find("ul>li>ul").removeClass("show").children("li:last-child").hide()}) //ocultar el boton al salir
    .on("mouseleave",function(){if(checked){checked=false;page_reload()}}) //ir a la nueva busqueda
    .on("click","button",page_reload)
    .parent("dl").find("dt>a").click(function(e) //boton todos
    {
        var id=$(this).data("filter");
        if(filtros[id]) //si tiene filtros los quita todos
        {
            delete(filtros[id]);
            $("#"+id+" li").removeClass("active");
            page_reload();
        }
        e.preventDefault();
    });
    //ventana de descarga
    $('#results').click(ocultar).on({ //si se pincha en cualquier parte de los resultados se oculta la ventana de descarga
        mouseenter:function()
        {
            var animation=function(thumb)
            {
                if(thumb.length && thumb.data("servers").length>2)
                    return setInterval(function()
                    {
                        var img=(parseInt(thumb.attr("src").substr(-1))+1)%thumb.data("servers").split("_").length;
                        thumb.attr("src","http://images"+thumb.data("servers").substr(img*3,2)+".foofind.com/"+thumb.data("id")+img);
                    },500);
            }
            this.thumb_animation=animation($(".thumblink img",this));
        },
        mouseleave:function(){if(this.thumb_animation) clearInterval(this.thumb_animation)}
    },">li").on("click",">li[id!=loading]",function(e) //mostrar ventana de descarga del archivo
    {
        $(this).data("scroll-start", $(window).scrollTop());
        $("#download").scrollTop(0).removeClass("top"); //reiniciar el estado de download solo en este caso
        mostrar($("h3 a",this),true);
        e.preventDefault();
        e.stopPropagation();
    }).on("click","a",function(e) //abrir ventana en pestaña
    {
        e.stopPropagation();
        //si no se usa el Ctrl a la vez que se hace clic o no es el enlace del archivo
        if(!e.ctrlKey && $(this).parent("h3").length)
        {
            e.preventDefault();
            mostrar($(this),true);
        }
    }).on("click",".thumblink",function(){$(this).data("active_play",true)});
    $("#fondo").hoverIntent({sensitivity:1,over:ocultar,interval:50,out:ocultar,timeout:50}); //ocultar fondo
    $("#flecha").click(function() //movimiento flecha
    {
        if($("#fondo").data("visible"))
            ocultar();
        else if($("+div>article",this).data("id"))
            mostrar($('li.active h3 a'),false)
    });
    $("#download").on("click",">button",function(e) //cerrar ventana
    {
        $(this).parents().removeClass("active");
        ocultar(200,true)
        e.stopPropagation();
    }).on("click","input",function(e) //autoseleccionar enlace del input
    {
        $(this).select();
        e.stopPropagation()
    }).on("click","#download_links",function(e) //desplegar los links
    {
        $(this).toggleClass("active");
        $("#download_share").slideUp();
        $("#share_download_links").removeClass("active");
        $("#sources_links").slideToggle();
        e.preventDefault();
        e.stopPropagation()
    }).on("click","#share_download_links",function(e) //desplegar share
    {
        $(this).toggleClass("active");
        $("#download_share+#sources_links").slideUp();
        $("#download_links").removeClass("active");
        $("#download_share").slideToggle();
        e.preventDefault();
        e.stopPropagation()
    }).on("click","#favorite",function(e) //favoritos
    {
        e.stopPropagation();
        if($(this).parent().children("#login_required").length) //si no esta logueado se muestra mensaje
            $(this).parent().children("#login_required").appendTo("#links").show();
        else //sino se ejecuta la peticion
            $.ajax(
            {
                url:lang+"favorite",
                data:$(this).data(),
                context:this
            }).done(function(r)
            {
                if(r["OK"]===true)
                {
                    $(this).toggleClass("active");
                    //intercambiar title
                    var the_title=$(this).attr("title");
                    $(this).attr("title",$(this).data("title"));
                    $(this).data("title",the_title);
                    //intercambiar action
                    if($(this).data("action")=="add")
                        $(this).data("action","delete");
                    else
                        $(this).data("action","add");
                }
                else //se ha deslogueado en otra pagina
                    window.location.reload();
            }).fail(function(r){$("#download h2").before(error(r))});
    }).on("click","#vote_up,#vote_down",function(e) //votos
    {
        e.stopPropagation();
        $.ajax(
        {
            url:lang+"vote",
            data:$(this).data(),
            context:this
        })
        .done(function(r)
        {
            $(this).parents("ul").find("button:gt(0)").attr("title",function(i,t) //cambiar el titulo
            {
                $(this).toggleClass("active",r["vote"]!=i); //se aprovecha para activarlo o no
                return t.substr(0,t.indexOf("("))+"("+r['c'][i]+")"
            })
        }).fail(function(r){$("#download h2").before(error(r))});
    }).on("click","#metadata>a",function() //desplegar los metadatos
    {
        var metadata=$(this).parent("section").children("dl");
        if(metadata.hasClass("active")) //no se usa toggleClass directamente porque hay bugs al colapsar la lista
            metadata.animate({height:125},500,function()
            {
                $(this).removeClass("active").removeAttr("style");
                $("#subcontent").css("height","auto");
            });
        else
            metadata.addClass("active",500,function()
            {
                var new_size=$(this).height()-200+$("#download").scrollTop()+$("#download").height()+subcontent_top;
                if($("#subcontent").height()<new_size)
                    $("#subcontent").css("min-height",new_size);
            });
    }).on("click","#comments>a",function(e) //desplegar los comentarios
    {
        var comments=$(this).parent("section").children("dl");
        if(comments.hasClass("active")) //no se usa toggleClass directamente porque hay bugs al colapsar la lista
            comments.animate({height:125},500,function(){$(this).removeClass("active").removeAttr("style")});
        else
            comments.addClass("active",500);
    }).on("click","dl+a",function(e) //intercambiar los mensajes de metadatos y comentarios despues de desplegarlos
    {
        var info=$(this).text();
        $(this).text($(this).data("info"));
        $(this).data("info",info);
        e.preventDefault();
        e.stopPropagation()
    }).on("click","textarea",function(e){e.stopPropagation()}) //evitar clic normal de download
    .hoverIntent({sensitivity:1,over:function(){mostrar($('li.active h3 a'),false)},out:function(){},interval:50}) //mostrar en el hover
    .click(function() //scroll interno
    {
        $(window).scrollTop($('#results li.active').data("scroll-start"));
        mostrar($('li.active h3 a'),false)
    });
    $("#share").hover(function(){$(this).children("button").addClass("active")},function(){$(this).children("button").removeClass("active")});

    $(window).scroll(function(e)
    {
        update_global_ui_vars();

        //la paginacion continua se activa cuando se baja por los resultados pero antes de llegar al final de la pagina
        load_more_results(true);

        var results = $("#results");
        var active = $("li.active", results);
        if(active.length) //si hay item activo acomoda la pestaña de download en la parte de arriba
        {
            var download=$("#download");
            download.css({position:"fixed",top:arriba<subcontent_top?subcontent_top-arriba:0});
            if($("#fondo").data("visible") || ancho>ancho_max_download) //si esta desplegado ademas se regula la derecha
                download.css({right:ancho>ancho_max_download?ancho-ancho_max_download:ancho<ancho_min_download?ancho-ancho_min_download:0});

            //scroll dentro de la descarga
            var max_scroll=download.prop("scrollHeight")-download.outerHeight(); //calcula la cantidad de scroll que se puede hacer en la descarga
            if (max_scroll<=0) //reinicia el scrollTop por si se ha redimensionado la ventana
                download.scrollTop(0);
            else //calcula el scroll que se debe hacer
            {
                var pos=arriba-active.data("scroll-start"); //posicion del scroll interno respecto de donde se empezó
                var offset_top=subcontent_top+(pos-arriba)<0?0:subcontent_top+(pos-arriba); //saca scroll-start de pos
                download.scrollTop(arriba<subcontent_top?0:pos<max_scroll+offset_top?pos-offset_top:max_scroll)
                .toggleClass("top",pos>2 && arriba>subcontent_top).toggleClass("bottom",pos<=max_scroll-2+offset_top);
            }
        }

        var last_item_top = $(window).scrollTop()+$(window).height();
        var items = results.children();
        var search_page = current_search_page*10;
        while(items[search_page] && items[search_page].id!="loading" && $(items[search_page]).position().top<last_item_top) {
            search_page+=1;
        }
        search_page = Math.floor(search_page/10);

        if (search_page>current_search_page) {
            current_search_page = search_page;
            track_pageview();
        }
    }).resize(function()
    {
        update_global_ui_vars();
        if($("#fondo").data("visible"))
        {
            $("#subcontent").css({width:ancho});
            $("#flecha").css({right:posicion_flecha});
            $("#download").css({position:"absolute",top:arriba<subcontent_top?0:arriba-subcontent_top,right:0,height:alto-55});
            if(ancho>=ancho_max_download) //si la ventana esta desplegada a la derecha
                $("#fondo").fadeOut(200).data("visible",false);
        }
        else
        {
            $("#subcontent").css({width:ancho-10});
            $("#flecha").css({right:ancho<ancho_min_download+20?posicion_flecha-500:ancho-ancho_max_download>60?posicion_flecha+50:ancho-840});
            $("#download").css(
            {
                position:"absolute",
                top:arriba<subcontent_top?0:arriba-subcontent_top,
                right:ancho<ancho_min_download+20?-500:ancho-ancho_max_download>60?50:ancho-ancho_max_download-10,
                height:alto-55
            });
        }
    }).bind(
    {
        hashchange:function() //actualiza download
        {
            update_current_url();
            track_pageview();
            search_params();
            if(current_url_info.has_download)
            {
                ocultar(0);
                mostrar($('#results h3 a[data-id="'+filtros["d"]+'"]'));
            }
            else
                ocultar(0,true);
        },
        popstate:function()
        {
            update_current_url();
            track_pageview();
            search_params();
            if(current_url_info.has_download) //si la nueva URL tiene download se muestra
            {
                ocultar(0);
                mostrar($('#results h3 a[data-id="'+filtros["d"]+'"]'));
            }
            else
                ocultar(0,true);
        }
    }).unload(function(){$(":input[disabled=disabled]").removeAttr("disabled")})
    //configuracion de las peticiones ajax
    $.ajaxSetup(
    {
        type:"post",
        beforeSend:function(xhr,settings)
        {
            if(settings.type=="POST")
                xhr.setRequestHeader("X-CSRFToken",csrf_token);
        }
    });
    //tambien si se pulsa en ver mas resultados
    $('#more').click(function(e)
    {
        search(true);
        e.preventDefault();
    });
    //hay que ocultar download porque esta fixed y no se puede poner la lista de idiomas encima al estar en absolute
    $('#select_language_box').hover(function(){$("#download").css("z-index",0);ocultar(0)},function(){$("#download").css("z-index",6)});

    if (current_url_info.has_download) //si es download se inicializa todo lo necesario
    {
        if($(".thumblink").data("play"))
            $(".thumblink").data("play",$(".thumblink").data("play").split(",")); //adaptar el play si viene

        if(!filtros["q"]) //si es download sin busqueda se guarda para poder poner de nuevo la URL original
            first_download={d:filtros["d"],dn:filtros["dn"]};

        if(window.history.pushState) //si el navegador soporta history se procede normalmente
        {
            devueltos=respuesta_total_found=1;
            archivos[filtros["d"]]=$("#download").html();
        }
        else //sino se redirige siempre
            change_url(true);

        mostrar($('#results h3 a[data-id="'+filtros["d"]+'"]'));
    }

    // Busco enlaces con estadisticas en results (por si vienen precargados)
    link_lookup($("#results"));
    link_lookup($("#download"));

    search();
    track_pageview();
});

function update_global_ui_vars(){
    alto=$(window).height();
    ancho=$(window).width();
    arriba=$(window).scrollTop();
}

var last_download="";
var last_search_page=-1;
function track_pageview()
{
    track_download = false;
    if (current_url_info.has_download) {
        if (current_url!=last_download) {
            _gaq.push(['_trackPageview',current_url]);
            last_download = current_url;
        }
    }

    if ((!current_url_info.has_download || current_search_page>0) && last_search_page<current_search_page)
    {
        // genera url de busqueda
        var search_info = clone(current_url_info);
        search_info.has_download = false;
        if (!current_url_info.has_search) {
            search_info.has_search = true;
        }
        search_url = format_url(search_info);

        // añade número de página a la url
        if (current_search_page>0) search_url += "?page="+current_search_page;
        last_search_page = current_search_page;

        _gaq.push(['_trackPageview',search_url]);
    }
}

function update_current_url(){
    // url sin barra final
    url = location.pathname
    var url_len = url.length;
    if(url.charAt( url_len-1 ) == "/") {
        url = url.slice(0,url_len-1);
    }

    // hash sin almohadilla ni hashbang
    hash = location.hash;
    if (hash && hash[0]=="#")
        if (hash[1]=="!")
            hash = hash.substr(2);
        else
            hash = hash.substr(1);

    // barra de separación con hash
    if (hash && hash[0]!="/")
        hash = "/"+hash;

    // parsea información de la url actual
    current_url_info = parse_url(url + hash);

    window.lang = current_url_info.lang;

    // genera la url actual a partir de la info actual, para unificar criterios
    current_url = format_url(current_url_info);

}

function clone(obj){
    return jQuery.extend(true, {}, obj);
}

function parse_url(url)
{
    var ret = Object();
    ret.lang = window.lang;

    var parts;
    if (window.location.host.endsWith("googleusercontent.com")) {
        var url_params = window.location.search.substr(1).split("&");
        for (param in url_params) {
            var url_param = url_params[param].split("=",2);
            if (["q", "u"].indexOf(url_param[0])!==-1)
            {
                parts = url_param[1].split(":").pop();
                while (parts[0]=="/") parts = parts.substr(1); // quita barras iniciales
                parts = parts.substr(parts.indexOf("/")+1); // quita nombre del dominio y barra inicial
            }
        }
        if (parts.endsWith("+")) // quita el + final
            parts = parts.substr(0, parts.length-1);
    } else {
        parts = url.substr(1); // quita la barra inicial
    }

    parts = parts.split("/");

    // quita la parte del lenguage
    if (parts[0].length==2) {
        ret.lang = "/"+parts[0]+"/";
        parts = parts.slice(1);
    }

    // información de busqueda
    ret.has_search = (parts[0]=="search");
    if (ret.has_search)
    {
        ret.search = {"q":decodeURIComponent(parts[1]).replace(/_/g," ")};
        parts = parts.slice(2);

        while (parts[0] && parts[0]!="download") {
            pair = parts[0].split(":",2);
            ret.search[pair[0]]=decodeURIComponent(pair[1]);
            parts = parts.slice(1);
        }
    }

    // información de descarga
    ret.has_download = (parts[0]=="download");
    if (ret.has_download) {
        ret.download = {"d":parts[1]};
        if (parts[2]) {
            ret.download["dn"] = decodeURIComponent(parts.slice(2).join("/"));
            if (ret.download["dn"].substring(ret.download["dn"].length - 5)!=".html")
                ret.download["dn"] += ".html"
        }
    }

    return ret;
}

/* genera una url a partir de la información de url dada
    hash_parts:  - undefined: no se usa hashbang
                 - false: solo parte del hashbang
                 - true: url completa con hashbang */
function format_url(info, hash_parts)
{
    var ret = (hash_parts==false)?"":lang;
    if (info.has_search && hash_parts!=false) {
        if (!info.search)
            info.search = {"q":$("#q").val()};
        else if (!("q" in info.search))
            info.search["q"] = $("#q").val();

        ret += "search/" + encodeURIComponent(info.search["q"].replace("/","_")).replace(/%20/g,"_");

        order=["src","type","size"];
        for(var key in order)
            if(order[key] in info.search)
            {
                ret+="/"+order[key]+":"+info.search[order[key]];
            }
    }

    if (info.has_download) {
        if (hash_parts!=undefined)
            ret += "#!/";
        else if (info.has_search)
            ret += "/";
        ret += "download/" + info.download["d"];
        if ("dn" in info.download) {
            ret += "/" + encodeURIComponent(info.download["dn"]);
        }
    }

    return ret;
}
