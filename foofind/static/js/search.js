var cargando,cargando150;
var filtros,first_download;
var last_items=pagina=devueltos=0; //necesario ponerlo tambien aqui para que funcione la paginacion de la primera busqueda
var archivos={};
var peticiones={},num_peticiones=0;
var alto,ancho,arriba,container,ancho_max_download=1440,ancho_min_download=930,posicion_flecha=610; //TODO min-width 940
var respuesta_total_found=-1;
var busqueda_programada=espera=0;
var hash=checked=false;

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
    for(var clave in filtros)
        if(jQuery.inArray(clave,["d","q","dn"])==-1)
        {
            add_slash=true;
            filtro+="/"+clave+":"+filtros[clave];
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
    var path = window.location.pathname.replace("/download/","/d:");
    if(path.slice(-1)=="/")
        path = path.slice(0,-1);

    var pathname=path.split("/").slice(lang=="/"?1:2);
    var section=pathname.shift();
    var query="";
    if(section=="search")
        query=["q:"+pathname[0]].concat(pathname.slice(1));
    else if(pathname[0])
        query=[section].concat(["dn:"+pathname[0]+(pathname[0].substr(-5)!=".html"?".html":"")]);
    else
        query=[section]

    if(window.location.hash && window.location.hash!="#")
        query=query.concat([window.location.hash.replace("#!/download/","d:")]);

    for(values=0;values<query.length;values++)
    {
        values_string = decodeURIComponent(query[values]);
        pair=values_string.split(":", 2);
        filtros[pair[0]]=pair[1].replace(/_/g," ");
    }

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
            $('#src a[data-filter^="'+sources[src]+'"]').parents("li").addClass("active");
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
    var url; //guarda la url nueva para compararla con la actual y cargarla si no coincide TODO arreglar cuando no viene dn
    var download="d" in filtros?"download/"+filtros["d"]+(first_download && first_download["dn"] && filtros["d"]==first_download["d"]?"/"+first_download["dn"]+(first_download["dn"].substr(-5)!=".html"?".html":""):""):"";
    if(change_download) //si hay que cambiar la URL de download
    {
        var filters=search_string(false);
        if(download!="" && filters.slice(-1)!="/") //separa download de texto de busqueda
            filters+="/";

        //añadir filtros a la URL si no hay download inicial y se esta en la busqueda normal o si hay download inicial y se vuelve a él
        url=lang+((!first_download && window.location.pathname.substr(4,6)=="search") || (first_download && filtros["d"]!=first_download["d"])?"search/"+filters:"")+download;
        if(window.history.pushState) //si el navegador soporta history
        {
            if(url!=window.location.pathname) //solo se actualiza si cambia la URL
                window.history.pushState(filtros,"",url);
        }
        else //sino se usa hashbang o se redirige
        {
            if(window.location.pathname.substr(4,6)==url.substr(4,6))
            {
                hash=true;
                window.location.hash=!first_download && download?"!/"+download:"";
            }
            else
                window.location=url;
        }
    }
    else
    {
        if(window.history.pushState)
        {
            filtros["q"]=$("#q").val(); //para que no se pierda la busqueda aunque no se le haya hecho submit
            url=lang+"search/"+search_string(false);
        }
        else
        {
            delete filtros["d"];
            url=lang+"search/"+search_string(false)+((download)?"#!/"+download:"");
        }
        window.location=url;
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
            espera+=respuesta["wait"]; //fecha en milisegundos de la proxima busqueda permitida
            if(respuesta["files_ids"].length)
                cargando=rotar(cargando,false); //si hay resultados parar ojitos

            for(var resp in respuesta["files_ids"]) //añadir resultado si no estaba ya antes
            {
                if(!results.find('>li>div>h3>a[data-id="'+respuesta["files_ids"][resp]+'"]').length) //evitar repetidos
                {
                    devueltos++;
                    results.append(respuesta["files"][resp]);
                    //precarga de miniaturas
                    var result=$(respuesta["files"][resp]).find(".thumblink img");
                    if(result.data("servers"))
                        for(var index in result.data("servers").split("_"))
                            $('<img>').attr("src",result.attr('src').slice(0,-1)+index);
                }
            }
            if(respuesta["total_found"]!=respuesta_total_found) //si no es paginacion
                if(respuesta["total_found"]==0 && respuesta["sure"] && !$("#download").hasClass("active")) //si no hay resultados ni download
                    results.html(respuesta["no_results"]);
                else //si hay resultados se muestra el mensaje resumen
                    $("#search_info").html(respuesta["result_number"]);

            //$("#related").empty().append(respuesta["tags"]); de momento no hay tags
            respuesta_total_found=respuesta["total_found"];
            more_results=load_more_results(respuesta["sure"]);
            if(more_results<0 && $("#results li:first").hasClass("loading")) //si es la ultima pagina posible y no hay resultados
                results.html(respuesta["no_results"]);
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
    if(!sure || pagina<respuesta_total_found) //si hay mas paginas para cargar
    {
        //si ya estan puestos todos los resultados que se han devuelto y la altura de los mismos es menor de lo esperado se busca
        if($('#results>li[class!="loading"]').length==devueltos && arriba>$("#results").height()-104*(arriba==0?3:5)-alto)
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
        if((paginacion || peticion!="paginacion") && peticiones[peticion] && peticiones[peticion].readyState!=4)
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
    alto=$(window).height();
    ancho=$(window).width();
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
                    if($('#results li.active h3 a').data("id")==id) //if paranoico para comprobar que se carga donde debe
                        download.html(data["html"]).addClass("active").toggleClass("bottom",(download.prop("scrollHeight")>download.outerHeight()));

                    archivos[id]=data["html"];
                    show_metadata_comments(download);
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
        download.css({position:"fixed",top:arriba<121?121-arriba:0,height:alto-35}).removeClass("top").show(100)
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
        $("#download").css({position:"fixed",top:arriba<130?$("#subcontent").position().top-arriba:0}).animate({right:ancho-ancho_max_download},{duration:duracion,queue:false});
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
    $("html").css("overflow-y","scroll");
    alto=$(window).height();
    ancho=$(window).width();
    arriba=$(window).scrollTop();
    container=$("#subcontent").position().top;
    //inicializar los ojitos
    cargando=rotar($(".loading"),false);
    cargando150=rotar($("#download>div"),false);
    //change necesario para inicializar
    $("#size div,#quality").slider({min:17,max:34,values:[17,34],range:true,stop:page_reload,change:function(e,ui){size_slider(ui.values,this)},slide:function(e,ui){size_slider(ui.values,this)}});
    search_params();
    //envio del formulario
    $('.searchbox').submit(function(e)
    {
        filtros["q"]=$("#q").val();
        $(":submit").attr("disabled","disabled")
        e.preventDefault();
        page_reload();
    })
    //filtros
    $('dd').on("click","a[href]",function(e) //TODO poner strong cuando no hay suborigenes activados y se activa uno
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
                if(thumb.length && thumb.data("servers"))
                    return setInterval(function()
                    {
                        var img=(parseInt(thumb.attr("src").substr(-1))+1)%thumb.data("servers").split("_").length;
                        thumb.attr("src","http://images"+thumb.data("servers").substr(img*3,2)+".foofind.com/"+thumb.data("id")+img);
                    },500);
            }
            thumb_animation=animation($(".thumblink img",this));
        },
        mouseleave:function(){clearInterval(thumb_animation)}
    },">li").on("click",">li[id!=no_results]",function(e) //mostrar ventana de descarga del archivo
    {
        $(this).data("scroll-start", $(window).scrollTop());
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
        $("#sources_links").slideToggle();
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
                    var title=$(this).attr("title");
                    $(this).attr("title",$(this).data("title"));
                    $(this).data("title",title);
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
                var new_size=$(this).height()-200+$("#download").scrollTop()+$("#download").height()+container;
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

    $(window).scroll(function(e)
    {
        arriba=$(window).scrollTop();
        //la paginacion continua se activa cuando se baja por los resultados pero antes de llegar al final de la pagina
        load_more_results(true);

        var active = $("#results li.active");
        if(active.length) //si hay item activo acomoda la pestaña de download en la parte de arriba
        {
            var download=$("#download");
            download.css({position:"fixed",top:arriba<container?container-arriba:0});
            if($("#fondo").data("visible") || ancho>ancho_max_download) //si esta desplegado ademas se regula la derecha
                download.css({right:ancho>ancho_max_download?ancho-ancho_max_download:ancho<ancho_min_download?ancho-ancho_min_download:0});

            //scroll dentro de la descarga
            var max_scroll=download.prop("scrollHeight")-download.outerHeight(); //calcula la cantidad de scroll que se puede hacer en la descarga
            if (max_scroll<=0) //reinicia el scrollTop por si se ha redimensionado la ventana
                download.scrollTop(0);
            else //calcula el scroll que se debe hacer
            {
                var pos=arriba-active.data("scroll-start"); //posicion del scroll interno respecto de donde se empezó
                var offset_top=container+(pos-arriba)<0?0:container+(pos-arriba); //saca scroll-start de pos
                download.scrollTop(arriba<container?0:pos<max_scroll+offset_top?pos-offset_top:max_scroll)
                .toggleClass("top",pos>2 && arriba>container).toggleClass("bottom",pos<=max_scroll-2+offset_top);
            }
        }
    }).resize(function()
    {
        alto=$(window).height();
        ancho=$(window).width();
        if($("#fondo").data("visible"))
        {
            $("#subcontent").css({width:ancho});
            $("#flecha").css({right:posicion_flecha});
            $("#download").css({position:"absolute",top:arriba<container?0:arriba-container,right:0,height:alto-35});
            if(ancho>=ancho_max_download) //si la ventana esta desplegada a la derecha
                $("#fondo").fadeOut(200).data("visible",false);
        }
        else
        {//TODO ancho_max_download+20
            $("#subcontent").css({width:ancho-10});
            $("#flecha").css({right:ancho<ancho_min_download?posicion_flecha-500:ancho-ancho_max_download>60?posicion_flecha+50:ancho-840});
            $("#download").css(
            {
                position:"absolute",
                top:arriba<container?0:arriba-container,
                right:ancho<ancho_min_download?-500:ancho-ancho_max_download>60?50:ancho-ancho_max_download-10,
                height:alto-35
            });
        }
    }).bind(
    {
        hashchange:function() //actualiza download
        {
            _gaq.push(['_trackPageview',location.pathname+location.hash]);
            if(!hash)
            {
                search_params();
                if(filtros["d"])
                {
                    ocultar(0);
                    $("#flecha,#download,#subcontent,#fondo").stop();
                    mostrar($('#results h3 a[data-id="'+filtros["d"]+'"]'));
                }
                else
                    ocultar(0,true);
            }
            hash=false;
        },
        popstate:function(event)
        {
            _gaq.push(['_trackPageview',location.pathname]); //actualizar analytics
            if(event.originalEvent.state!=null) //para evitar que se ejecute en la carga de pagina con chrome
            {
                if(event.originalEvent.state && event.originalEvent.state.d) //si la nueva URL tiene download se muestra
                {
                    search_params();
                    ocultar(0);
                    mostrar($('#results h3 a[data-id="'+filtros["d"]+'"]'));
                }
                else //sino se ocultar a la derecha
                    ocultar(0,true);
            }
        }
    }).unload(function(){$(":input[disabled=disabled]").removeAttr("disabled")})
    $.ajaxSetup(
    {
        type:"POST",
        beforeSend:function(xhr, settings)
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
    if(window.location.pathname.indexOf("/download/")!=-1) //si es download se inicializa todo lo necesario
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
    search();
});
